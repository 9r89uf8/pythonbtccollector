# Shadow-Signal Dashboard Design

Status: dashboard design specification. The required read-only reporting API is
implemented in this repository and awaits the schema-first production rollout;
the dashboard has not been implemented.

The dashboard will live in a separate repository and run only on the user's
laptop. The collector, PostgreSQL, Redis, shadow worker, and read-only FastAPI
application remain on the droplet.

## 1. Product Goal

The first dashboard should answer one question clearly:

> During one five-minute market, how close was each projected Chainlink price
> to the Chainlink price actually observable at that forecast's target time?

It must support:

- a **Live** view that grows through the active five-minute market;
- a **Recent** view for reviewing a completed market from start to end; and
- a direct visual comparison between actual and projected Chainlink prices.

The first version is a model-observation tool, not a general trading terminal.
Do not add probabilities, order entry, alerts, positions, flow, order-book
panels, or automatic model selection yet.

Every screen must retain the label **Shadow / Experimental**. The signal is a
short-horizon Chainlink catch-up projection. It is not a probability,
settlement prediction, confidence interval, market-close forecast, or trading
recommendation.

## 2. Decisions

Use:

- Vite's `vanilla` template;
- plain JavaScript, with no TypeScript and no frontend framework;
- Apache ECharts for the chart;
- `decimal.js` for financial calculations and formatting;
- Vitest for deterministic unit tests; and
- plain CSS with design tokens.

Do not use:

- React, Vue, or another component framework in the first version;
- Docker or a service on the droplet;
- browser access to PostgreSQL or Redis;
- a public FastAPI listener;
- CORS changes merely for the dashboard; or
- JavaScript `Number` for financial calculations.

ECharts supports timestamp axes and dashed line styling without requiring a
date adapter. The dataset is small: the evaluator schedules at most about 600
attempts per model in one five-minute market. Keep exact decimal strings in
application state, use `Decimal` for arithmetic, and convert values to numbers
only at the chart-rendering boundary. Tooltips and calculated labels must use
the original decimal values, not chart-coordinate floats.

References:

- [Vite getting started](https://vite.dev/guide/)
- [Vite development proxy](https://vite.dev/config/server-options.html#server-proxy)
- [Vite preview proxy](https://vite.dev/config/preview-options.html#preview-proxy)
- [Apache ECharts time axis](https://echarts.apache.org/handbook/en/concepts/axis/)
- [Apache ECharts line styling](https://echarts.apache.org/handbook/en/how-to/chart-types/line/basic-line/)
- [Apache ECharts accessibility](https://echarts.apache.org/handbook/en/best-practices/aria/)
- [`decimal.js`](https://github.com/MikeMcl/decimal.js)

## 3. Backend Contract

### What already exists

The current backend already provides:

| Purpose | Endpoint | Store |
| --- | --- | --- |
| Discover current and recent markets | `GET /markets?limit=10&include_current=true` | PostgreSQL |
| One-second market context | `GET /markets/{market_id}/data` | PostgreSQL |
| Current one-second market context | `GET /markets/current/data` | PostgreSQL |
| Observed source opening values | `GET /markets/{market_id}/sources` | PostgreSQL |
| Current source opening values | `GET /markets/current/sources` | PostgreSQL |
| Current actual price and latest projection | `GET /markets/current/live` | Redis |
| Current matured forecast history | `GET /markets/current/shadow-evaluations?model_version=...` | PostgreSQL |
| One market's matured forecast history | `GET /markets/{market_id}/shadow-evaluations?model_version=...` | PostgreSQL |

The live endpoint supplies only the latest short-lived shadow projection. It
cannot reconstruct a completed market after a page refresh.

PostgreSQL already stores the required paired forecast and outcome evidence in
`shadow_signal_evaluations`. The base table remains deliberately revoked from
`price_reader`; the reporting routes read only its restricted chart view.

### Evaluation reporting API

After the schema-first rollout, the backend provides these bounded,
PostgreSQL-backed routes:

```http
GET /markets/current/shadow-evaluations?model_version=catchup_ratio_l3000_b100
GET /markets/{market_id}/shadow-evaluations?model_version=catchup_ratio_l3000_b100
```

Their complete deployed contract is documented in [`FRONTEND_API.md`](FRONTEND_API.md).

The accepted query values are `catchup_ratio_l3000_b100`,
`catchup_ratio_l3500_b100`, and `catchup_ratio_l4000_b100`. The dashboard still
requests only the configured frozen primary.

Do not merge the 500 ms evaluation rows into `/markets/{market_id}/data`; that
route has a different one-second grid. Keep the reporting contract separate.

Each route is bounded to exactly one five-minute target window and one
explicit model version. It must not accept an arbitrary time range or an
unbounded limit. Return every attempt, including invalid and unscored attempts,
so the chart can show honest gaps. A normal response has at most roughly 600
rows; reject an anomalous result above 1,000 instead of silently truncating it.

For a known market with no retained evaluations, the API returns HTTP `200`
with `points: []`. The clock-derived current market also returns `200` before
its first `market_windows` row exists, including through the ID-addressed route.
HTTP `404` is reserved for an unknown non-current market.

### Model-selection rule

All three candidates are evaluated, but the dashboard must display only the
frozen accepted primary. It must never rank candidates or switch to whichever
model performed best recently.

The current primary is:

```text
catchup_ratio_l3000_b100
```

The evaluation table does not contain an `is_primary` column. For the first
dashboard, configure the model explicitly and verify that it matches
`signals.chainlink_catchup.model_version` and
both `selection_fingerprint_sha256` and `selection_artifact_sha256` whenever
the live signal is available. The fingerprint identifies the frozen policy and
inputs; the artifact hash identifies the complete selection decision.
Requiring `model_version` in the historical request is safer than pretending
the database records which candidate was primary in every past deployment.

Only call historical points the accepted primary when their one fingerprint
and artifact-hash pair matches the currently verified live pair and the
configured model matches the live model. When an older market has a different
pair, label the line `Configured candidate — historical primary unverified`;
the existing table alone cannot prove the old identity-to-primary mapping. If
one market contains more than one selection identity, fail closed to
actual-only context instead of joining projections across a selection change.

If the configured and live model versions differ, show a prominent
configuration-mismatch banner, hide the projection line and live ghost, and do
not resume projection rendering until the configuration is corrected.

### Required chart-point contract

The authoritative complete endpoint response, including the required additive
`performance.cohorts` object, is documented in
[`FRONTEND_API.md`](FRONTEND_API.md#persisted-shadow-evaluation-chart-data).
The chart-focused excerpt below shows the point and coverage fields used by
this design without duplicating the performance contract.

```json
{
  "schema_version": 1,
  "server_time_ms": 1783989305000,
  "market": {
    "market_id": 5946630,
    "market_start_ms": 1783989000000,
    "market_end_ms": 1783989300000,
    "boundary": "[start_ms,end_ms)"
  },
  "model": {
    "model_version": "catchup_ratio_l3000_b100",
    "horizon_ms": 3000,
    "beta": "1",
    "evaluation_cadence_ms": 500,
    "selection_identities": [
      {
        "fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
        "artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699"
      }
    ]
  },
  "coverage": {
    "window_buckets": 600,
    "market_window_elapsed": true,
    "observed_buckets": 600,
    "unobserved_buckets_as_of_response": 0,
    "attempts": 600,
    "valid_forecasts": 584,
    "scored": 579,
    "invalid": 16,
    "valid_without_actual": 5
  },
  "points": [
    {
      "selection_fingerprint_sha256": "2e403435a541b7fd7e431dc38ebeee62f88743c63ce8043088361fe7ac61b749",
      "selection_artifact_sha256": "890a08366d45cb33978f1c382f2030b62a50281a3606a4caa7ddfac3e1570699",
      "model_version": "catchup_ratio_l3000_b100",
      "beta": "1",
      "generated_ms": 1783989000507,
      "target_ms": 1783989003507,
      "matured_ms": 1783989003512,
      "horizon_ms": 3000,
      "valid": true,
      "status": "valid",
      "invalid_reasons": [],
      "state": "anchored",
      "forecast_market_id": 5946630,
      "full_horizon_before_forecast_market_end": true,
      "chainlink_at_forecast": "64080.47",
      "projected_chainlink": "64103.07",
      "actual_chainlink": "64099.82",
      "actual_chainlink_source_timestamp_ms": 1783989003000,
      "actual_chainlink_received_ms": 1783989003340,
      "actual_chainlink_age_at_target_ms": 167,
      "pending_move": "22.60",
      "pending_move_bps": "3.526815580472490292",
      "direction": "up",
      "forecast_error": "3.25",
      "baseline_error": "-19.35"
    }
  ]
}
```

All financial values remain JSON strings or `null`. A scored point requires:

```text
valid = true
projected_chainlink is not null
actual_chainlink is not null
```

A valid forecast can remain unscored when no causal actual was available.
Invalid attempts have `projected_chainlink`, `pending_move`, direction, and
related projection fields set to `null`.

`full_horizon_before_forecast_market_end` is an API alias for the stored
`full_horizon_before_market_end`. It describes the generation-time market, not
necessarily the requested target-time market. The alias prevents boundary-
crossing forecasts from being misread.

`evaluation_cadence_ms` is required for detecting a worker observation bucket
that produced no row. It is metadata, not a promise that `generated_ms` is
exactly aligned to a 500 ms timestamp.

`model.horizon_ms` and `model.beta` remain available when `points` is empty.
The API validates that every returned point has that same model version,
horizon, and beta before serializing the response.

The coverage bucket counts make leading and trailing absence visible even
though there is no row from which the frontend could draw a separator. The
API computes `observed_buckets` from distinct retained generation-bucket IDs.
For a live window, `unobserved_buckets_as_of_response` is `null`; future targets
must not appear to be missing. Once `market_window_elapsed` is true, that field
is `window_buckets - observed_buckets`. It is deliberately an as-of-response
observation, not a finality claim. Absence can mean a skipped observation,
restart, queue drop, rejected write, database delay, or retention.

Every actual response also groups its derived per-market performance by the
exact selection fingerprint and artifact pair. Cohorts use only valid points
with causal actuals, their `scored_points` sum to `coverage.scored`, and an
identity with no scored points has null metrics. With no retained points,
`performance.cohorts` is empty. A dashboard must never blend multiple
selection identities into one claimed-primary score.

Detailed futures inputs and `created_at` are not needed in the first dashboard
contract.

### Temporal query rule

The requested chart window is selected by forecast target time:

```text
market_start_ms <= target_ms < market_end_ms
```

Do not select solely by the evaluation row's stored `market_id`; that field
describes `generated_ms`. A forecast generated during the preceding market's
last four seconds can target the beginning of the requested market. Likewise,
a forecast generated near the requested market's end can target the next
market.

For the current fixed horizons, the query can inspect rows whose generation
market is the requested market or its predecessor and then apply the exact
target-time boundary. If longer horizons are added later, add an index on
`(model_version, target_ms)`.

### Database security

`price_reader` has no direct access to `shadow_signal_evaluations`. The
implemented owner-rights reporting view is:

```text
shadow_signal_evaluation_chart_points
```

The schema enforces these requirements, and future migrations must preserve
them:

- keep the base table revoked from `PUBLIC` and `price_reader`;
- expose only the fields required by the chart through the view;
- revoke the view from `PUBLIC`;
- grant `SELECT` on the view to `price_reader`; and
- keep FastAPI on `READ_DATABASE_URL` with no writer credentials.

The API query parameterizes both `market_id` and `model_version`.

## 4. Screen Design

The chart is the product. It should receive most of the viewport instead of
being surrounded by many small market cards.

Desktop/laptop layout:

```text
┌ Oracle Catch-Up ─ SHADOW / EXPERIMENTAL ─ API ● ─ UTC time ┐
│ [ Live ] [ Recent ]   [‹] 21:00–21:05 UTC [›]  [Refresh]   │
├───────────────────────────────────────────────┬─────────────┤
│                                               │ Signal      │
│ Actual vs Projected Chainlink                 │ summary     │
│ Fixed five-minute chart                       │             │
│                                               │ Coverage    │
│                                               │ summary     │
├───────────────────────────────────────────────┴─────────────┤
│ Model · horizon · selection identity · retention notice   │
└─────────────────────────────────────────────────────────────┘
```

On a narrow window, place the signal summary below the chart. Do not reduce the
chart to less than roughly half the visible page height.

### Header and controls

Show:

- `Oracle Catch-Up`;
- the permanent `Shadow / Experimental` badge;
- `Live` and `Recent` mode buttons;
- the selected market's exact UTC start and end;
- previous and next market buttons based on discovered IDs;
- a refresh button in Recent mode;
- API/tunnel status; and
- a UTC clock or market countdown derived from API `server_time_ms`.

Preserve selection in the URL:

```text
/?mode=live
/?mode=recent&market=5946630
```

Use market IDs returned by `GET /markets`. Do not assume subtracting one from a
market ID always finds an available market.

### Visual style

Use a restrained research-console appearance, not a casino-style trading UI.

Suggested tokens:

```css
:root {
  --page: #080c12;
  --panel: #111821;
  --panel-raised: #16202b;
  --grid: #263341;
  --text: #ecf2f8;
  --muted: #8d9aaa;
  --actual: #42c7e8;
  --projected: #f4b860;
  --baseline: #8793a3;
  --threshold: #a78bfa;
  --positive: #4fd18b;
  --negative: #ff7474;
}
```

Use the system sans-serif stack and tabular numerals. Use a monospace stack for
timestamps, prices, model versions, and diagnostics. Do not load a remote font.

Color cannot be the only distinction: actual is solid, projected is dashed,
baseline is dotted, and the live ghost uses a hollow marker.

## 5. Chart Contract

### Axes

- Fix the x-axis to the selected half-open five-minute target window.
- Display elapsed labels from `00:00` through `05:00` and show exact UTC in the
  tooltip.
- Keep the market end as a boundary; a target exactly at `market_end_ms`
  belongs to the next market.
- Use a tight price y-axis with visible padding.
- Do not smooth either price series.
- Do not connect across `null` values.

### Series

| Series | Value | X coordinate | Style |
| --- | --- | --- | --- |
| Paired actual Chainlink | `actual_chainlink` | `target_ms` | Solid cyan, no persistent markers |
| Projected Chainlink | `projected_chainlink` | `target_ms` | Dashed amber, no persistent markers |
| No-change baseline | `chainlink_at_forecast` | `target_ms` | Faint dotted gray, hidden by default |
| Current live projection | `projected_chainlink` from `/live` | `generated_ms + horizon_ms` | Hollow amber ghost marker |
| Market opening threshold | Official or observed opening value | Full width | Thin purple horizontal line |

The paired evaluation `actual_chainlink` is the exact causal outcome used for
scoring: the newest Chainlink cache observation seen by the evaluator during a
successful worker observation, with `received_ms <= target_ms`. The worker
normally observes every 100 ms, but an event overwritten in Redis entirely
between observations cannot be reconstructed. This paired value is the correct
actual for matching the stored score; raw replay remains the event-complete
authority.

Use `showSymbol: false` for the paired actual, projected, and baseline series.
Expose exact points through the axis crosshair and tooltip, and reserve the
hollow persistent marker for the live ghost. This keeps roughly 600 points per
series legible on a laptop.

`GET /markets/{market_id}/data` can provide a lighter one-second contextual
Chainlink line, especially when no evaluation rows are retained. Do not use
that rounded source-second series to recalculate forecast accuracy; use the
paired evaluation actual and stored error.

Request this contextual route with its default `fill_display=false`. Carrying a
prior Chainlink value forward would conceal genuine observation gaps.

### The non-negotiable alignment rule

Plot the projection at `target_ms`, never at `generated_ms`:

```text
target_ms = generated_ms + horizon_ms
```

For the accepted model, the target is currently three seconds after
generation. Plotting at generation time would shift every forecast three
seconds early and make the chart misleading.

`matured_ms` is persistence timing. It is not a chart coordinate.

### Meaning of the dashed line

The dashed curve connects a sequence of independent endpoint forecasts for
successive target times. It is not a forecast of the path Chainlink will take
between generation and target.

Place this sentence directly below the legend:

> Dashed values are independent three-second target forecasts, not a predicted
> continuous price path.

### Missing and invalid attempts

Return invalid attempts and insert `null` at their target times so ECharts
breaks the dashed line. Never carry a preceding projection through an invalid
period. A valid projection without a paired actual remains on the dashed line,
but the actual line is `null` there and the point is excluded from scoring.

A missing persisted evaluation bucket has no returned database row. Detect it
from adjacent
`Math.floor(generated_ms / evaluation_cadence_ms)` bucket numbers, not from
exact `target_ms` spacing. When adjacent returned buckets differ by more than
one, insert a `null` separator between their target coordinates in every
evaluation-derived line. Call this an `unobserved retained bucket`; the chart
cannot infer whether the cause was scheduling, restart, queue pressure, a
write failure, rejection, or another operational condition. Do not invent a
price or an evaluation attempt. A missing leading or trailing bucket is
represented by coverage, not by a fake endpoint.

### Market opening threshold

Use this priority:

1. `market.chainlink_resolution.open` from the data endpoint when non-null;
   label it `Official market open`.
2. The `open` value for provider `polymarket_chainlink_rtds` from the sources
   endpoint; label it `Observed window open`.
3. If neither exists, omit the line.

Never label an observed fallback as official.

### Tooltip

At a forecast target, show:

```text
Target                 21:00:03.507 UTC
Projected Chainlink           $64,103.07
Actual at target              $64,099.82
Forecast error                    +$3.25
Absolute error                     $3.25
No-change error                  -$19.35
Generated                     3.0 seconds earlier
Status                         valid
```

Use `decimal.js` and the stored decimal strings for every displayed
calculation.

## 6. Live Mode

Live mode automatically follows the current market.

Anchor every refresh cycle to one market ID so a five-minute rollover cannot
mix responses from two markets:

1. Fetch `GET /markets/current/live` first.
2. Take its top-level `market_id`, `market_start_ms`, and `market_end_ms` as the
   live window identity.
3. Fetch `GET /markets/{market_id}/data`,
   `GET /markets/{market_id}/sources`, and the
   `GET /markets/{market_id}/shadow-evaluations?...` route for that exact ID.
4. Fetch `GET /markets?limit=10&include_current=true` separately for
   navigation.

Do not issue several `/markets/current/...` context requests in parallel. The
boundary could occur between their individual market-ID resolutions.

Default polling:

| Request | Interval |
| --- | --- |
| `/markets/current/live` | 1 second |
| ID-addressed shadow evaluations | 2 seconds |
| ID-addressed `/data` and `/sources` | 5 seconds |
| Market discovery | 30 seconds |

Use a recursive `setTimeout` only after the previous request settles. Do not
use `setInterval` or allow overlapping requests. Pause polling while the tab is
hidden, abort requests when the mode or market changes, and use bounded retry
delays of 1, 2, 4, then 5 seconds. On `visibilitychange` back to visible, run
one immediate anchored refresh before resuming the normal cadence.

Use `cache: "no-store"` for live fetches. Do not display a stale value as live
or append a stale ghost point. Completed-market requests can use normal HTTP
caching later if the API adds explicit cache validators.

Use API `server_time_ms`, not the laptop clock, for countdowns and rollover.
When a live response reports a new `market_id`, immediately abort all requests
for the old ID, remove its ghost, replace the fixed chart window, and start the
ID-addressed fetches again.

### Live actual freshness

The live cache can retain a non-null price while a source is disconnected. For
the initial dashboard, use the deployed shadow policy's Chainlink receive-age
limit of 2,500 ms and a conservative provider source-age limit of 5,000 ms:

- `prices.chainlink.value === null`: show `Actual price unavailable`;
- both `received_age_ms <= 2500` and `source_age_ms <= 5000`: show `Live`;
- either age over its limit: show `Stale` and dim the value.

Show both ages in the tooltip. The receive-age limit matches the model's
staleness policy; the separate source-age limit prevents a newly received but
provider-old event from appearing fresh. Keep both limits in one dashboard
configuration module and label them in diagnostics. A missing Redis key is a
successful HTTP `200` response with null Chainlink fields, not an API failure.
A fresh live actual is contextual and must not be substituted for a paired
evaluation outcome when calculating accuracy.

In phase one, show the `/live` Chainlink value only in the signal side card.
Do not append it to the paired-actual series: it has live-cache timing rather
than the evaluator's `actual_chainlink @ target_ms` semantics. The chart's
newest actual comes from matured pairs or the separately labeled one-second
context line. A future live-actual overlay needs its own series contract and is
out of scope here.

### Live ghost

When `signals.chainlink_catchup` is present and `valid=true`:

```text
ghost_target_ms = generated_ms + horizon_ms
```

Show a hollow ghost marker only when its target belongs to the selected target
window. The signal's nested market fields remain generation-time context and
must appear in the tooltip when the forecast crosses a boundary.

Do not connect the ghost to the actual price with a diagonal line. That would
imply a forecast trajectory the model does not produce.

Deduplicate live and persisted points by:

```text
model_version + generated_ms + horizon_ms
```

Remove the ghost when the matching matured evaluation appears.

### Signal summary

```text
ORACLE CATCH-UP             SHADOW / EXPERIMENTAL

Chainlink at signal                    $64,080.47
Projected Chainlink (+3.0s)            $64,103.07
Pending catch-up                          +$22.60
Pending catch-up                          +3.53 bps

Catch-up direction                             UP
Signal freshness                            75 ms
Full horizon before generation market end      YES
```

Use `Projected Chainlink (+3.0s)`, not `Futures-implied`. Do not show an
“expected absorption 1–3 sec” range; the current model predicts one endpoint
at a fixed horizon.

If the live signal is well-formed but `valid=false`, show its `status` and
`invalid_reasons` and remove the ghost. Never reuse the preceding valid
projection.

If the shadow payload is missing, expired, or malformed, the current API
returns HTTP `200` with `signals.chainlink_catchup: null` while preserving
actual prices. Treat all three cases as `Live projection unavailable`, not as
an actual-price or tunnel failure.

## 7. Recent Mode

Load recent markets from:

```http
GET /markets?limit=10&include_current=false
```

For the selected market, fetch the data, sources, and evaluation
endpoint in parallel. Completed markets are static and should be fetched once
unless the user presses Refresh.

The default Recent list contains completed markets only. If the active market
is offered as a convenience later, put it in a separately labeled `Live now`
group rather than presenting it as a completed review.

The first version should show:

- the full fixed five-minute chart;
- actual and projected target-time lines;
- the opening threshold when available;
- paired/scored coverage counts;
- the configured model and horizon; and
- point-level error in the tooltip.

Do not show a generic “accuracy percentage.” The backend now returns tested
per-selection performance cohorts with forecast-error, no-change-baseline,
skill, and paired comparison fields. Dashboard consumption belongs in the
separate dashboard repository and must follow the calculation and presentation
rules in `FRONTEND_API.md`, including the active-market “so far” label and
scored coverage.

Evaluation retention is currently 168 hours. If a known market has no retained
points, show:

```text
No projection history retained for this market. Actual Chainlink data is still
available.
```

Then render the one-second actual-only context instead of treating the empty
forecast response as an application error.

## 8. Application States

Track live Redis data, evaluation history, one-second context, and opening
sources as independent resources. Failure of one request must not clear data
that came from another healthy request.

| Condition | Dashboard behavior |
| --- | --- |
| Loading | Preserve chart dimensions and show a quiet skeleton |
| SSH tunnel/API offline | Keep the last chart dimmed; show `API unavailable` and last-success time |
| Missing/malformed/expired shadow payload | HTTP `200`; continue actual data, hide ghost, and show `Live projection unavailable` |
| `valid=false` | Show status/reasons; insert a projection gap; never reuse old projection |
| Live Chainlink fields are null | HTTP `200`; show `Actual price unavailable`, not `API unavailable` |
| Live Chainlink receive/source age exceeds 2,500/5,000 ms | Mark the side-card value `Stale` and dim it |
| Paired actual observation missing | Leave an actual gap; do not interpolate or fabricate it |
| Historical `points: []` | Show retention/no-evidence message and actual-only context |
| Evaluation route fails while `/live` works | Preserve the live side card and ghost; show `Projection history unavailable` |
| `/live` fails while evaluations work | Preserve the last paired chart, remove/dim the ghost, and show `Live feed unavailable` |
| `/data` fails while evaluations work | Keep the paired actual/projected chart; mark one-second context unavailable |
| Recent selected `/data` returns `404` | Refresh discovery and prompt for another market |
| Live `/data` returns `404` just after rollover | Keep the new empty window and retry; do not jump back to the completed market |
| `/sources` returns `404` | Omit the observed-opening fallback; retain the market and chart |
| HTTP `503` live route | Mark only the live feed unavailable and retry with bounded backoff |
| Market rollover | Move Live mode to the new target window and add the completed market to Recent |
| Configured/live model mismatch | Show a configuration banner and render actual-only context |
| Multiple selection identities in one market | Show a selection-change banner and render actual-only context |
| Opening value unavailable | Omit threshold instead of inventing one |

## 9. Laptop Access and Vite Proxy

FastAPI remains private at `127.0.0.1:9000` on the droplet and has no CORS
middleware. From PowerShell or another terminal with OpenSSH, forward laptop
port `9000` to droplet port `9000`:

```bash
ssh -N -L 9000:127.0.0.1:9000 DROPLET_USER@DROPLET_IP
```

Keep that terminal open. The dashboard browser should call `/api/...`; Vite
proxies those requests to `http://127.0.0.1:9000`, the same concrete forwarded
port. Replace only `DROPLET_USER` and `DROPLET_IP` in the command.

Never expose droplet port `9000`, `5432`, or `6379` publicly to make dashboard
development easier.

## 10. Project Setup

Create the dashboard in its own repository:

```bash
npm create vite@latest shadow-signal-dashboard -- --template vanilla
cd shadow-signal-dashboard
npm install
npm install echarts decimal.js
npm install --save-dev vitest jsdom
```

Use a currently supported Node.js release as required by the installed Vite
version.

Create an ignored `.env.local`:

```dotenv
API_PROXY_TARGET=http://127.0.0.1:9000
VITE_PRIMARY_MODEL_VERSION=catchup_ratio_l3000_b100
VITE_CHAINLINK_RECEIVED_STALE_MS=2500
VITE_CHAINLINK_SOURCE_STALE_MS=5000
```

These dashboard settings are not secrets. Never place passwords, SSH keys,
database URLs, or other secrets in any `VITE_*` variable because Vite exposes
those variables to browser code. Parse and validate both age limits as positive
base-10 integers in `src/config.js`; do not silently accept `NaN`.

Use this `vite.config.js`:

```javascript
import { defineConfig, loadEnv } from "vite";

function apiProxy(target) {
  return {
    "/api": {
      target,
      changeOrigin: true,
      rewrite: (path) => path.replace(/^\/api/, ""),
    },
  };
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "API_");
  const target = env.API_PROXY_TARGET ?? "http://127.0.0.1:9000";

  return {
    server: {
      host: "127.0.0.1",
      port: 5173,
      strictPort: true,
      proxy: apiProxy(target),
    },
    preview: {
      host: "127.0.0.1",
      port: 4173,
      strictPort: true,
      proxy: apiProxy(target),
    },
    test: {
      environment: "jsdom",
    },
  };
});
```

Add these scripts to `package.json`:

```json
{
  "scripts": {
    "test": "vitest run",
    "test:watch": "vitest"
  }
}
```

Keep the Vite-generated `dev`, `build`, and `preview` scripts alongside them.

Run:

```bash
npm run dev
```

Open only:

```text
http://127.0.0.1:5173
```

## 11. Suggested Project Structure

```text
src/
  main.js
  config.js
  api/
    client.js
    markets.js
    shadowEvaluations.js
  state/
    dashboardStore.js
  controllers/
    liveController.js
    recentController.js
    polling.js
  domain/
    marketWindow.js
    shadowSeries.js
    decimalFormat.js
  charts/
    catchupChart.js
    catchupChartOptions.js
  views/
    header.js
    marketControls.js
    signalCard.js
    coverageStrip.js
    statusBanner.js
  styles/
    tokens.css
    layout.css
    components.css
tests/
  fixtures/
  shadowSeries.test.js
  marketWindow.test.js
  polling.test.js
```

Keep API parsing, domain transformation, chart options, and DOM rendering
separate. The chart module should receive normalized point arrays, not raw API
responses.

## 12. Data Transformation Rules

For each evaluation attempt:

```javascript
const targetBelongsToMarket =
  point.target_ms >= market.market_start_ms &&
  point.target_ms < market.market_end_ms;

const hasProjection =
  point.valid === true && point.projected_chainlink !== null;

const scored =
  hasProjection &&
  point.actual_chainlink !== null;
```

Create chart points at `target_ms`. Invalid attempts must insert a `null`
projection at their target so the dashed line breaks. A valid projection whose
actual is unavailable remains visible as an unscored projection, while its
actual series has a gap. Do not count it in accuracy metrics.

Keep API decimal strings alongside chart coordinates:

```javascript
import Decimal from "decimal.js";

function financialChartNumber(value) {
  return value === null ? null : new Decimal(value).toNumber();
}

{
  targetMs: point.target_ms,
  projectedDecimal: point.projected_chainlink,
  actualDecimal: point.actual_chainlink,
  projectedPlotValue: financialChartNumber(point.projected_chainlink),
  actualPlotValue: financialChartNumber(point.actual_chainlink),
}
```

The null guard is mandatory: `Number(null)` is zero and would draw a false
zero-dollar crash. Use the same null-preserving conversion for every financial
series.

Before building the series, sort attempts by `generated_ms`. For each adjacent
pair, compute:

```javascript
const previousBucket = Math.floor(previous.generated_ms / cadenceMs);
const nextBucket = Math.floor(next.generated_ms / cadenceMs);
```

When `nextBucket - previousBucket > 1`, add a null separator at an x-coordinate
strictly between the two real target times. This separator is only a rendering
instruction; it must not affect attempt, validity, or score counts.

The numeric copies exist only for pixel positioning. Tooltip arithmetic,
signed error, absolute error, margin to the opening threshold, and future
summary metrics must use `Decimal` constructed from the string fields.

## 13. Accessibility

- Import ECharts' ARIA component and enable `aria.show`.
- Provide a text summary of the selected market and visible series.
- Add a keyboard-accessible table of recent scored points below the visual
  chart, collapsible by default.
- Make every control keyboard operable with visible focus.
- Announce API disconnection and recovery through an
  `aria-live="polite"` region.
- Use line patterns and markers in addition to color.
- Use UTC explicitly in visible time labels.
- Respect `prefers-reduced-motion` and disable rapid chart animation.
- Do not rely on red and green alone for direction or error.

## 14. Testing

### Unit tests

Block release unless tests prove:

- projection uses `target_ms`, never `generated_ms`;
- actual and projected comparison points share the same x-coordinate;
- target-time half-open market boundaries are correct;
- a forecast generated in the preceding market can enter the selected target
  window;
- a forecast targeting exactly `market_end_ms` is excluded;
- invalid attempts break the projected line;
- valid projections without an actual remain visible but unscored;
- null financial values remain null and never become numeric zero;
- an unobserved retained 500 ms bucket inserts a visual separator without
  creating a fake attempt;
- decimal calculations use `decimal.js`;
- `null` or invalid live signals remove the ghost;
- the ghost is deduplicated when its matured record appears;
- polls never overlap and are aborted on mode changes; and
- rollover replaces the fixed five-minute window cleanly without mixing
  ID-addressed context from the preceding market;
- model, selection fingerprint, or artifact-hash mismatches fail closed to
  actual-only context;
  and
- stale and missing live Chainlink states remain distinct from API failure.

Use committed fixtures for:

- a completed market with scored points;
- a live market with one future ghost;
- invalid attempts;
- missing causal actuals;
- target-time boundary crossings;
- an empty retained-evaluation response; and
- tunnel/API failure and recovery.

### Build checks

```bash
npm test
npm run build
npm run preview
```

The preview server must remain bound to `127.0.0.1` and must use the same local
API proxy.

## 15. Build Order

1. Implement and deploy the restricted evaluation reporting view and the two
   read-only API routes. **Implementation complete; production rollout is in
   `OPERATIONS.md`.**
2. Add focused backend tests and update `FRONTEND_API.md` with the real
   response contract. **Implemented in this repository.**
3. Create the separate Vite vanilla-JavaScript repository.
4. Add static API fixtures and implement target-time transformations first.
5. Build Recent mode and verify a completed five-minute market end to end.
6. Add Live mode, the future ghost, non-overlapping polling, and rollover.
7. Add failure states, accessibility, and deterministic tests.
8. Run `npm test` and `npm run build`, then compare the dashboard against the
   API through the SSH tunnel.

Do not start by building the chart from browser-captured live points alone.
That would create a demo that loses its evidence on refresh and cannot review a
recent completed market.

## 16. Phase-One Acceptance Criteria

Phase one is complete when:

- the dashboard runs entirely on the laptop;
- Vite and preview bind only to `127.0.0.1`;
- all API calls go through the Vite proxy and SSH tunnel;
- a user can choose Live or a discovered recent market;
- the chart always spans exactly one five-minute target window;
- actual and projected Chainlink are aligned at `target_ms`;
- invalid attempts and missing observations remain visible gaps;
- a live forecast appears as an unconnected ghost and later becomes a matured
  point;
- a page refresh can reconstruct a recent market from PostgreSQL-backed API
  data;
- the configured model is never dynamically reranked, and it is called the
  frozen primary only when its selection identity can be verified;
- all financial calculations use decimal strings and `decimal.js`;
- API/tunnel loss is explicit and never silently presented as fresh data;
- the UI remains clearly labeled `Shadow / Experimental`; and
- unit tests and the production build pass.
