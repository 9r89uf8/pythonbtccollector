# Shadow-Signal Performance Dashboard Handoff

Status: frontend integration specification. The backend response is implemented
in the price-collector repository. The dashboard remains in its separate
repository; this document does not add dashboard code here.

The authoritative endpoint and field contract is
[`FRONTEND_API.md`](FRONTEND_API.md#persisted-shadow-evaluation-chart-data).
The broader dashboard layout and chart rules remain in
[`SHADOW_SIGNAL_DASHBOARD_DESIGN.md`](SHADOW_SIGNAL_DASHBOARD_DESIGN.md).

## 1. Product Name and Meaning

Call the panel:

```text
Forecast performance
```

For the active market, call it:

```text
Forecast performance so far
```

Keep the permanent `Shadow / Experimental` context near it. A useful subtitle
is:

```text
3.0 s horizon · 147 scored targets
```

Do not call the panel `Accuracy`, `Model accuracy`, or `Accuracy percentage`.
The backend reports continuous Chainlink price errors in USD and comparisons
against a no-change baseline. It does not report classification accuracy,
settlement accuracy, probability accuracy, profitability, or confidence.

Use `coverage.market_window_elapsed` to choose between the `So far` and
completed states. Do not infer completion from the laptop clock or merely from
whether the dashboard is in Live or Recent mode.

## 2. API Call

There is no separate performance endpoint. Both existing evaluation routes
return the points, coverage, and `performance.cohorts` in one response:

```http
GET /markets/current/shadow-evaluations?model_version=catchup_ratio_l3000_b100
GET /markets/{market_id}/shadow-evaluations?model_version=catchup_ratio_l3000_b100
```

The configured primary is currently:

```text
catchup_ratio_l3000_b100
```

Pass that complete model version in `model_version`. The visible `3.0 s` label
comes from the returned `model.horizon_ms`; `3s` is not the query value and
`2.5s` is not a supported evaluation model.

The browser must call the private droplet API through the dashboard's
same-origin `/api` proxy and SSH tunnel. Do not expose droplet port `9000` or
add CORS merely for this panel.

### Live mode

Keep the dashboard's existing market-anchored refresh sequence:

1. Fetch `/api/markets/current/live`.
2. Read its returned `market_id`.
3. Fetch
   `/api/markets/{market_id}/shadow-evaluations?model_version=...`.
4. Poll that exact ID-addressed evaluation route every two seconds while the
   market remains active.

This prevents a five-minute rollover from mixing the live signal, chart, and
performance from adjacent markets. Schedule the next poll only after the
previous one settles, abort old requests on rollover, and pause while the tab
is hidden.

The `/markets/current/shadow-evaluations` route is valid for a standalone
follow-current view, but its returned `market.market_id` is authoritative. Once
the dashboard has anchored or pinned a market, use the ID-addressed route.

### Recent mode

For a selected completed market, fetch the ID-addressed evaluation route once
alongside the existing market-data and source requests. Refresh it only when
the user presses Refresh, or when the dashboard deliberately allows a short
post-close grace period for delayed persistence.

Do not issue another request to calculate performance. Do not recalculate the
aggregate metrics from chart coordinates.

### Browser helper

This example is compatible with the existing vanilla-JavaScript dashboard and
same-origin proxy:

```javascript
const SUPPORTED_MODELS = new Set([
  "catchup_ratio_l3000_b100",
  "catchup_ratio_l3500_b100",
  "catchup_ratio_l4000_b100",
]);

export const CONFIGURED_MODEL_VERSION =
  import.meta.env.VITE_PRIMARY_MODEL_VERSION;

if (!SUPPORTED_MODELS.has(CONFIGURED_MODEL_VERSION)) {
  throw new Error("VITE_PRIMARY_MODEL_VERSION is missing or unsupported");
}

export async function fetchShadowEvaluationReport(
  marketId,
  { signal, live = false } = {},
) {
  if (!Number.isSafeInteger(marketId) || marketId < 0) {
    throw new TypeError("marketId must be a non-negative safe integer");
  }

  const path = `/api/markets/${marketId}/shadow-evaluations`;
  const url = new URL(path, window.location.origin);
  url.searchParams.set("model_version", CONFIGURED_MODEL_VERSION);

  const response = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
    cache: live ? "no-store" : "default",
    signal,
  });

  const contentType = response.headers.get("content-type") ?? "";
  const body = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const detail = typeof body === "object" && body !== null
      ? body.detail ?? body.error
      : body;
    const error = new Error(detail || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }

  if (typeof body !== "object" || body === null) {
    throw new Error("Shadow evaluation response is not an object");
  }
  if (body.market?.market_id !== marketId) {
    throw new Error("Shadow evaluation response market mismatch");
  }
  if (body.model?.model_version !== CONFIGURED_MODEL_VERSION) {
    throw new Error("Shadow evaluation response model mismatch");
  }
  if (body.performance?.cohorts === undefined) {
    throw new Error("API update required: performance field is absent");
  }

  return body;
}
```

Call it from the existing controllers like this:

```javascript
const liveReport = await fetchShadowEvaluationReport(live.market_id, {
  signal: liveAbortController.signal,
  live: true,
});

const recentReport = await fetchShadowEvaluationReport(selectedMarketId, {
  signal: recentAbortController.signal,
});

renderForecastPerformance({
  market: liveReport.market,
  model: liveReport.model,
  coverage: liveReport.coverage,
  cohorts: liveReport.performance.cohorts,
});
```

Item 7 raises `schema_version` to `2` because selection provenance, outcome
integrity, and evaluation-causality semantics change how rows are interpreted.
During rollout, schema 1 or an absent `performance` field means the dashboard
reached an older backend. Show `API update required`; do not treat it as an
empty market.

## 3. Response Fields Used by the Panel

Each entry in `performance.cohorts` covers one full selection identity: schema
version, policy version, evidence end, fingerprint, and artifact hash. In the
normal case there is one cohort.

| UI label | Response field |
| --- | --- |
| Average absolute error (MAE) | `forecast.mean_absolute_error_usd` |
| Typical absolute error (median) | `forecast.median_absolute_error_usd` |
| P95 absolute error | `forecast.p95_absolute_error_usd` |
| Largest absolute error | `forecast.maximum_absolute_error_usd` |
| RMSE | `forecast.root_mean_squared_error_usd` |
| Average bias | `forecast.mean_signed_error_usd` |
| No-change baseline MAE | `no_change_baseline.mean_absolute_error_usd` |
| No-change baseline RMSE | `no_change_baseline.root_mean_squared_error_usd` |
| MAE advantage vs no change | `mean_absolute_advantage_usd` |
| MAE change vs no change | `mae_skill_vs_no_change` |
| RMSE change vs no change | `rmse_skill_vs_no_change` |
| Closer/equal/worse counts | `paired_comparison.wins/ties/losses` |
| Closer/equal/worse rates | `paired_comparison.win_rate/tie_rate/loss_rate` |
| Metric sample size | `scored_points` |

The no-change baseline assumes the forecast-time Chainlink value remains
unchanged until the target. It is not the market-opening price.

The performance sample includes only attempts where:

```text
valid = true
outcome_status = available
forecast_error is present
baseline_error is present
```

The report-level
`evaluation_semantics.scored_input_max_future_skew_ms` must be `0`. It is a
live-evaluator scoring rule independent of selection schema: v2-derived live
rows still use zero-skew scoring and are not directly comparable to v2 replay
evidence that allowed nonzero skew.

Show the returned aggregate values. The backend calculated them with exact
Decimal arithmetic from the persisted paired errors. The frontend should not
recompute them from rounded labels, chart pixels, `/markets/{id}/data`, or the
latest Redis price.

## 4. Recommended Layout

Keep the price chart dominant. In Live mode, place this card in the right rail
below the live signal and above or beside coverage. In Recent mode, a full-width
strip immediately below the chart works well. On narrow screens, move the card
below the chart.

Example desktop card:

```text
FORECAST PERFORMANCE                                      SO FAR
3.0 s horizon · 147 scored targets · Shadow / Experimental

Average absolute error (MAE)                       $3.25
No-change baseline MAE                            $19.35
MAE change vs no change                       83.2% lower
MAE advantage vs no change                  $16.10 closer

Typical abs. error     P95 absolute error     Largest absolute error
$2.10                  $9.80                  $13.07

Closer than no change                  112 W · 3 equal · 32 L
                                                       76.2%

Bias: $0.42 high · RMSE: $4.20 · 147 scored / 160 attempts

Descriptive for this five-minute market; overlapping forecasts.
```

Recommended hierarchy:

1. Make forecast MAE and no-change MAE the primary pair.
2. Put the MAE comparison directly below them in plain language.
3. Keep median, p95, and largest error visible; these explain whether the
   average hides occasional large misses.
4. Show the paired closer/equal/worse result and its scored count.
5. Put RMSE, signed bias, RMSE skill, exact hashes, and diagnostic counts in a
   `Details` disclosure when space is tight.

Do not use a radial gauge, progress ring, or bounded 0–100 score for skill.
Skill can be negative and is not an accuracy percentage.

Use semantic `<dl>` name/value pairs for the metric grid. Keep the details
disclosure keyboard accessible and expose a text summary to assistive
technology.

## 5. Metric Copy and Tooltips

Use these explanations:

| Label | Tooltip |
| --- | --- |
| Average absolute error (MAE) | Average absolute difference between projected and causally observed Chainlink prices. |
| Typical absolute error (median) | Half of scored absolute errors were at or below this value. |
| P95 absolute error | The empirical nearest-rank 95th percentile of scored absolute errors. It is not a confidence bound. |
| Largest absolute error | Largest observed absolute forecast error in this cohort and market. |
| RMSE | Error measure that gives larger misses more weight than MAE. |
| Average bias | Mean signed error. Positive means projections averaged above actual; negative means below. |
| No-change baseline | Error if Chainlink had remained at its forecast-time value until each target. |
| MAE change vs no change | Relative change in MAE versus the paired no-change baseline. It can be lower, equal, or higher. |
| Closer than no change | Share of paired targets where forecast absolute error was smaller than no-change absolute error. |

Interpret signed values in words:

- positive `mean_absolute_advantage_usd`: `$X closer on average`;
- negative `mean_absolute_advantage_usd`: `$X worse on average`;
- positive skill: `X% lower error vs no change`;
- negative skill: `X% higher error vs no change`;
- zero skill: `Same error as no change`;
- positive mean signed error: `$X high on average`;
- negative mean signed error: `$X low on average`; and
- zero mean signed error: `No average signed bias`.

For advantage, skill, and bias copy, display the absolute magnitude because
`closer`, `worse`, `lower`, `higher`, `high`, or `low` already communicates
direction. This avoids double-sign copy such as `-$2.00 worse`.

`paired_comparison.win_rate` is a closer-than-baseline rate, not direction
accuracy. Show the counts with it so the denominator is always visible.
Color may reinforce `closer`, `worse`, `high`, and `low`, but those words must
carry the meaning without color.

## 6. Decimal Formatting

All financial metrics and rates are strings or `null`. Keep those strings in
state and use `decimal.js` for formatting, sign checks, comparisons, and
percentage conversion. Do not parse financial values with `Number`,
`parseFloat`, or unary `+`.

Usually show USD errors to two decimal places and percentages to one decimal
place. Keep the unrounded string available in a tooltip or diagnostic view.
If a nonzero magnitude rounds below one cent, show `<$0.01` rather than
`$0.00`. Format a magnitude first, then attach field-specific wording such as
`closer`, `worse`, `high`, `low`, `lower`, or `higher`.

```javascript
import Decimal from "decimal.js";

function groupedFixed(value) {
  const [whole, fraction] = value.split(".");
  return `${whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}.${fraction}`;
}

export function formatUsdMagnitude(value) {
  if (value === null) return "—";

  const exact = new Decimal(value).abs();
  const rounded = exact.toDecimalPlaces(2);
  if (!exact.isZero() && rounded.isZero()) {
    return "<$0.01";
  }

  return `$${groupedFixed(rounded.toFixed(2))}`;
}

export function formatRatioMagnitudeAsPercent(value) {
  if (value === null) return "N/A";
  return `${new Decimal(value).abs().times(100).toDecimalPlaces(1).toFixed(1)}%`;
}

export function describeMaeAdvantage(value) {
  if (value === null) return "N/A";
  const decimal = new Decimal(value);
  if (decimal.isZero()) return "Same MAE as no change";
  return `${formatUsdMagnitude(value)} ${decimal.isPositive() ? "closer" : "worse"}`;
}

export function describeMaeChange(value) {
  if (value === null) return "N/A — no-change error was zero";
  const decimal = new Decimal(value);
  if (decimal.isZero()) return "Same error as no change";
  return `${formatRatioMagnitudeAsPercent(value)} ${decimal.isPositive() ? "lower" : "higher"}`;
}

export function describeBias(value) {
  if (value === null) return "N/A";
  const decimal = new Decimal(value);
  if (decimal.isZero()) return "No average signed bias";
  return `${formatUsdMagnitude(value)} ${decimal.isPositive() ? "high" : "low"} on average`;
}
```

For a null skill caused by zero baseline error, display:

```text
N/A — no-change error was zero
```

Do not display null as zero.

## 7. Make Sudden Misses Visible

Keep the existing actual-versus-projected price chart. Add a compact signed
forecast-error strip directly below it as part of this frontend checkpoint:

- x-coordinate: `point.target_ms`;
- y-value: the persisted `point.forecast_error`;
- zero line: forecast exactly matched the causal actual;
- positive value: projection was above actual;
- negative value: projection was below actual; and
- invalid or unscored point: gap, never a carried value.

This makes a sudden `+$13` or `-$13` miss visible at the exact target time even
when it is subtle on a high-price BTC chart. Use the original error string in
the tooltip and convert to a JavaScript number only at the final chart-coordinate
boundary. A muted no-change-error overlay may be available as a toggle, but it
should not obscure the forecast error.

Plot and score at `target_ms`, never `generated_ms`. A point near the start of
the requested target window can legitimately have been generated in the
preceding market.

## 8. Selection Identity Rules

Do not assume `performance.cohorts[0]` is the primary. Cohorts are sorted by
identity, not ranked by quality.

Compare the complete identity:

```text
selection_identity.schema_version
selection_identity.policy_version
selection_identity.evidence_end_ms
selection_identity.fingerprint_sha256
selection_identity.artifact_sha256
```

For Live mode, the verified identity is available from:

```text
signals.chainlink_catchup.selection_schema_version
signals.chainlink_catchup.selection_policy_version
signals.chainlink_catchup.selection_evidence_end_ms
signals.chainlink_catchup.selection_fingerprint_sha256
signals.chainlink_catchup.selection_artifact_sha256
```

If the live signal is unavailable and the dashboard has no independently
configured full identity pair, treat the cohort as unverified, not as a
mismatch and not as implicitly primary. In an active market, fail closed to
actual-only context until an identity can be verified.

When exactly one cohort matches the verified pair, render the normal headline
card and label its short identity in Details, for example:

```text
Selection 2e403435… / artifact 890a0836…
```

Use shortened hashes only for display. Compare and retain all 64 characters,
and make the full pair copyable.

If an active market's one cohort differs from the verified live identity, show:

```text
Configured/live selection mismatch
```

Hide the headline performance, projected series, and signed-error strip. Keep
the causal actual context visible. Do not downgrade an exact mismatch into an
unverified historical label.

For a completed historical market whose one cohort cannot be verified as that
market's old primary, label it:

```text
Configured candidate — historical primary unverified
```

If more than one cohort exists:

- show `Selection changed during this market`;
- do not create one combined headline score;
- keep the evaluation chart actual-only, following the existing fail-closed
  dashboard rule;
- show one separately labeled performance card per cohort if the user expands
  the diagnostic section;
- include each cohort's short hashes and `scored_points`; and
- never average, sum, rank, or choose the best cohort.

The sum of all cohort `scored_points` must equal `coverage.scored`, but that
invariant does not authorize combining their metrics.

## 9. Coverage and Sample Size

Always place sample size next to performance:

```text
147 scored · 160 attempts · 8 invalid · 5 valid without actual
```

Map `valid without actual` to `coverage.valid_without_actual`. These matured
attempts had a valid projection but no causal paired actual, so they remain
unscored. Inspect each point's `outcome_status`: `unavailable` is ordinary
absence, while `integrity_invalid` has one or more explicit
`outcome_invalid_reasons`. Neither is a failed prediction or paired loss.

Do not use `window_buckets` as the performance denominator. Restart duplicates
can create more than one attempt in a 500 ms bucket. Use returned
`scored_points`, `coverage.scored`, and `coverage.attempts` exactly as defined.

When only a few points are scored, let the visible scored count communicate the
limited evidence. Do not hide p95 merely because it equals the maximum; that is
expected for small samples. Avoid a claim of statistical significance at any
sample size because adjacent 500 ms forecasts overlap and are strongly
autocorrelated.

## 10. UI States

| Response condition | Dashboard behavior |
| --- | --- |
| Initial request | Preserve card dimensions and show a quiet skeleton. |
| Active market with `cohorts: []` and no points | `Waiting for persisted forecast attempts.` |
| Completed market with `cohorts: []` | `No retained forecast performance for this market.` Keep the actual-price chart. |
| Active cohort has `scored_points: 0` | `No causally scored forecasts yet.` Show em dashes, never `$0.00` or `0%`. |
| Completed cohort has `scored_points: 0` | `No causally scored forecasts for this market.` Show invalid/unscored coverage. |
| Active market with metrics | Add the `So far` badge and refresh values without animated counting. |
| Valid forecasts have no paired actual | Show the `valid without actual` count; do not count them as losses. |
| Skill is null and baseline error is zero | `N/A — no-change error was zero.` |
| Active cohort identity differs from verified live identity | Show `Configured/live selection mismatch` and fail closed to actual-only context. |
| More than one cohort | Show a selection-change banner and separate identities; never aggregate. |
| `performance` is absent | `API update required.` This is an older backend, not empty evidence. |
| HTTP `404` | `Market not found.` Refresh market discovery. |
| HTTP `422` | Configuration/request error. Stop retrying until the model or market input is corrected. |
| HTTP `500` or network failure | `Evaluation reporting unavailable.` Preserve other healthy dashboard data. |
| Refresh fails with prior data | Keep prior metrics dimmed with `Stale — last updated …`; do not replace them with zeros. |
| Request was aborted during navigation | Suppress the error and render the newly selected market. |

Use `server_time_ms` for the report refresh timestamp and current/completed
state. It is response time, not the timestamp of the newest evaluation.

## 11. Runtime Validation

Treat the endpoint as untrusted input at the browser boundary. Validate before
rendering at least these invariants:

- `schema_version === 2`;
- `evaluation_semantics.scored_input_max_future_skew_ms === 0`;
- returned market ID equals the requested ID;
- returned model version equals the configured model;
- `performance.cohorts` exists and is an array;
- all SHA-256 values are lowercase 64-character hexadecimal strings;
- market IDs and millisecond timestamps are non-negative safe integers;
- the market is a 300,000 ms half-open window;
- every point satisfies `target_ms === generated_ms + horizon_ms` and its
  target belongs to the returned market window;
- every point has complete selection schema/policy/evidence/hash provenance;
- `available` outcomes have an actual and no outcome-invalid reasons,
  `unavailable` outcomes have neither, and `integrity_invalid` outcomes have
  no actual plus at least one explicit reason;
- points are sorted by target time, generation time, then horizon;
- financial metrics are finite decimal strings or `null`;
- `points.length === coverage.attempts` and is at most 1,000;
- `valid_forecasts + invalid === attempts`;
- `scored + valid_without_actual === valid_forecasts`;
- the set of cohort identities exactly matches
  `model.selection_identities`;
- cohort identities are unique;
- the cohort `scored_points` sum equals `coverage.scored`;
- `wins + ties + losses === scored_points` for every cohort;
- a zero-scored cohort has null metrics/rates and zero paired counts; and
- with scored points, all metrics and rates are present except a skill whose
  matching no-change denominator is zero.

Use a runtime schema validator if the dashboard already has one. Allow unknown
additive fields, but require every field this panel consumes. If validation
fails, isolate the failure to the performance/evaluation panel; do not clear a
healthy Redis live signal or one-second actual chart.

## 12. Acceptance Checklist

The frontend checkpoint is complete when tests demonstrate:

- the configured model version is sent on every evaluation request;
- Live mode anchors the evaluation request to the `/live` market ID;
- completed markets use the ID-addressed route without continuous polling;
- Decimal strings never pass through binary floating-point calculations for
  labels or comparisons;
- positive, zero, negative, and null advantage/skill states have correct copy;
- mean signed error displays `high`, `low`, or neutral correctly;
- empty, unscored, retained-data-expired, older-backend, and request-error
  states remain distinct;
- p95 can equal maximum without being treated as an error;
- active metrics say `So far` and completed metrics do not;
- full selection identities are compared even though shortened hashes are
  displayed;
- an active full-identity mismatch hides headline performance, projections,
  and the signed-error strip;
- multiple cohorts never produce an aggregate score or silently select the
  first cohort;
- paired counts and cohort scored totals are validated;
- a `$13` signed-error spike is plotted at `target_ms`;
- invalid and unscored points produce gaps rather than zeros or carry-forward;
  and
- failure of this PostgreSQL reporting route does not clear healthy live data.

The final UI should answer two questions without overstating the evidence:

1. How far were the forecasts from the causally observed Chainlink targets in
   this five-minute market?
2. Were they closer than simply assuming Chainlink would not move?
