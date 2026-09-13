# Leader-risk extraction and tabulation

The [blueprint](../../H3_TWAP_LEADER_RISK_STUDY.md) defines the study. Use
available valid 60-second history from **2026-08-16 00:00 UTC**. Supplied start
and end boundaries are honored without a hidden date clamp. The end is
exclusive: market starts precede it and market closes are at or before it.
September 10 at 02:30 UTC is one descriptive comparison marker.

## Run from PowerShell

Run from this repository's root with standard-library Python 3.12. Choose a
new directory for every export. The example covers August 16 through
September 12 at 21:15 UTC. It reads production data in one repeatable-read,
read-only transaction; it installs no code, dependencies or database objects.

```powershell
$h3StartUtc = '2026-08-16T00:00:00Z'
$h3EndUtc = '2026-09-12T21:15:00Z'
$h3RunDir = 'results\leader_risk\example-full-history-run'
$h3Timeout = '180s'
$h3StartMs = [DateTimeOffset]::Parse($h3StartUtc).ToUnixTimeMilliseconds()
$h3EndMs = [DateTimeOffset]::Parse($h3EndUtc).ToUnixTimeMilliseconds()
if ($h3StartMs % 300000 -ne 0 -or $h3EndMs % 300000 -ne 0 -or
    $h3EndMs -le $h3StartMs) { throw 'Use aligned UTC market boundaries' }
if (Test-Path -LiteralPath $h3RunDir) { throw 'Choose a new run directory' }
New-Item -ItemType Directory -Path $h3RunDir -ErrorAction Stop | Out-Null
$h3Partial = Join-Path $h3RunDir 'observations.csv.partial'
$h3SqlHash = (Get-FileHash -LiteralPath 'research\leader_risk\extract.sql' -Algorithm SHA256).Hash.ToLowerInvariant()
$h3Extraction = [ordered]@{
    start_utc = $h3StartUtc
    end_utc_exclusive = $h3EndUtc
    statement_timeout = $h3Timeout
    sql_sha256 = $h3SqlHash
    read_only = $true
    isolation = 'repeatable read'
    psql_exit_code = $null
}
$h3Extraction | ConvertTo-Json |
    Set-Content -LiteralPath (Join-Path $h3RunDir 'extraction.json') -Encoding utf8
Get-Content -LiteralPath 'research\leader_risk\extract.sql' -Raw |
    ssh -o BatchMode=yes -o ConnectTimeout=12 root@152.42.247.86 "sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -v start_utc=$h3StartUtc -v end_utc=$h3EndUtc -v statement_timeout=$h3Timeout -d price_collector" |
    Set-Content -LiteralPath $h3Partial -Encoding utf8 -ErrorAction Stop
$h3Exit = $LASTEXITCODE
$h3Extraction.psql_exit_code = $h3Exit
$h3Extraction | ConvertTo-Json |
    Set-Content -LiteralPath (Join-Path $h3RunDir 'extraction.json') -Encoding utf8
if ($h3Exit -ne 0) { throw "Extraction failed (exit $h3Exit); retain partial output" }
python research/leader_risk/tabulate.py $h3Partial --output $h3RunDir `
    --expected-start $h3StartUtc --expected-end $h3EndUtc `
    --expected-sql-sha256 $h3SqlHash
if ($LASTEXITCODE -ne 0) { throw 'Tabulation validation failed; inspect partial output' }
Move-Item -LiteralPath $h3Partial -Destination (Join-Path $h3RunDir 'observations.csv') -ErrorAction Stop
```

Only accept an export after the native process exits zero and the tabulator
passes. It verifies eight unique checkpoints per stored market and compares
the distinct market count with `cohort_market_count`, computed in the same
snapshot before expanding checkpoints. Thus a truncated export containing
whole eight-row groups still fails. Requested bounds must match the CSV.
Calendar slots lacking stored metadata are reported explicitly in the
manifest, separately from stored markets whose inputs are unavailable. The
full-history cohort has startup slots before the first stored market.

The query defaults to a 60-second statement timeout. This full-history runner
explicitly chooses `180s`; there is no automatic retry or timeout increase.
Source tables are scanned for a conservative safety check before indexed
source-market joins are used. In the **same snapshot**, every expected-source
row must have `received_ms - sample_second_ms` in **[-2999, 177000] ms**
(TWAP nanoseconds are floored to milliseconds). Together with the existing
source-table market constraints and these fixed checkpoints, that proves no
candidate receipt can have a different source market. The check runs before
source freshness filtering, and fails closed with no accepted CSV if any row
violates the bound, even outside the requested dates. Investigate such a
failure and use receipt-only selection; do not simply remove the guard while
keeping source-market equality. A newer stale arrival must never reveal an
older fresh sample.

## Results and checks

`tabulate.py` produces [eight full-history grids](../../results/leader_risk/2026-09-12-full-history/report.md)
and these artifacts:

- `grids.csv`: all 160 cells, including empty cells, with N, U, L, resolved
  denominator, loss rate and pointwise 95% Wilson bounds.
- `comparison_cells.csv` and `comparison_totals.csv`: one descriptive pre/post
  split at September 10, 02:30 UTC, alongside the full-sample primary result.
- `direction_cells.csv`, `direction_totals.csv`, `daily_cells.csv`, and
  `daily_totals.csv`: leader and UTC market-start-day breakdowns.
- `coverage.csv`, `exclusions.csv`, and `outcome_accounting.csv`: unique
  unavailable counts, overlapping reasons, ties, inclusive freshness-edge
  counts and outcome status/type/rule accounting for each input group.
- `manifest.json`: requested cohort, calendar missingness, outcome snapshot
  timestamp, counts, conventions, verification and raw/SQL/code/output hashes.
  `extraction.json` records the extraction process exit and SQL hash beforehand.

All financial calculations use Decimal with precision 60. A cell is eligible
only when inputs are available and W differs from K. Exactly 3,000 ms passes
both source and receipt age; nanosecond TWAP receipts are compared exactly to
the millisecond cutoff. K must be an unambiguous opening event received by the
checkpoint. Prices are not converted to floating point.

X/Y bins use the blueprint's exact edges. Each cell counts N eligible rows,
U without a verified official winner and L known leader losses. The resolved
denominator is `n = N - U`; rates and intervals are unavailable when n is zero.
The two-sided pointwise 95% Wilson calculation uses
`z = Decimal('1.959963984540054')`, `p = Decimal(L)/n`,
`a = 1 + z*z/n`, `center = (p + z*z/(2*n))/a`, and
`half = z * (p*(1-p)/n + z*z/(4*n*n)).sqrt()/a`, clipped to [0,1].
CSV rates retain calculation precision; percentages are rounded only in the
Markdown display. No checkpoint pooling or threshold selection is performed.

Per T, total rows must equal unavailable inputs plus available ties plus
eligible rows. Cell counts conserve N, U and L against their checkpoint totals;
N equals resolved plus U and L cannot exceed resolved. Unknown outcomes among
excluded rows are accounted for separately from cell U. Individual exclusion
reasons can overlap and must not be summed as a unique excluded total.

Raw `observations.csv` and `.partial` files are ignored by Git. Preserve the
summaries, extraction record and manifest. A rerun can see newly resolved
outcomes; it is a new dated snapshot, not a replay of an old outcome state.
Do not overwrite a previous run's artifacts.

Run focused checks with:

```powershell
python -m pytest tests/test_h3_leader_risk_extract_guard.py tests/test_leader_risk_tabulate.py
```

## Add market bid and ask prices

The [official consolidated report](../../H3_TWAP_LEADER_RISK_FINAL_REPORT.md) contains the final results and findings for the settlement study, price addition, and gross-return/exclusion audit. Earlier dated outputs remain immutable.

The [market-price companion](../../results/leader_risk/2026-09-13-market-prices/report.md)
adds Polymarket leader-token quotes to the frozen observations. It preserves
the original settlement outcomes and compares them with quotes from a separately
recorded read-only snapshot. No new market-data collection or production update
is required.

For each checkpoint, `extract_quotes.sql` selects the latest retained quote
snapshot in the preceding ten seconds with both its sample key and row receipt
no later than the cut. It exports token-identity checks and all four individual
bid/ask source and receipt clocks. The Python companion tests freshness after
selection, so a newer stale component cannot expose an older fresh value.

The default **3,000 ms inclusive** quote-age limit applies to the selected
sample, its row receipt, and each leader bid/ask component's provider and receipt age. Primary
price comparisons require both sides fresh, valid prices, matching tokens,
and bid <= ask. Independent ask/bid availability and exclusions are also
counted. This paired-quote population is stricter than the earlier review's
ask-only diagnostic.

These are retained one-second snapshots. The probability writer skips samples
when an ask is missing, so a later quote withdrawal can leave an older priced
snapshot in history. The age checks cannot reconstruct those omitted updates.
Fresh retained quotes are a descriptive benchmark, not proof of an offer still
available at the cutoff. The newer 100 ms evidence is needed for a finer
execution-timing study.

Run the following from the repository root, using a new output directory:

```powershell
$h3StartUtc = '2026-08-16T00:00:00Z'
$h3EndUtc = '2026-09-12T21:15:00Z'
$h3BaseCsv = 'results\leader_risk\2026-09-12-full-history\observations.csv'
$h3QuoteDir = 'results\leader_risk\example-market-prices'
if (Test-Path -LiteralPath $h3QuoteDir) { throw 'Choose a new run directory' }
New-Item -ItemType Directory -Path $h3QuoteDir -ErrorAction Stop | Out-Null
$h3QuoteSqlHash = (Get-FileHash -LiteralPath 'research\leader_risk\extract_quotes.sql' -Algorithm SHA256).Hash.ToLowerInvariant()
$h3QuoteRecord = [ordered]@{
    start_utc = $h3StartUtc
    end_utc_exclusive = $h3EndUtc
    statement_timeout = '60s'
    sql_sha256 = $h3QuoteSqlHash
    read_only = $true
    isolation = 'repeatable read'
    psql_exit_code = $null
}
$h3QuoteRecord | ConvertTo-Json |
    Set-Content -LiteralPath (Join-Path $h3QuoteDir 'extraction.json') -Encoding utf8
Get-Content -LiteralPath 'research\leader_risk\extract_quotes.sql' -Raw |
    ssh -o BatchMode=yes -o ConnectTimeout=12 root@152.42.247.86 "sudo -u postgres psql -X -q -v ON_ERROR_STOP=1 -v start_utc=$h3StartUtc -v end_utc=$h3EndUtc -v statement_timeout=60s -d price_collector" |
    Set-Content -LiteralPath (Join-Path $h3QuoteDir 'observations.csv.partial') -Encoding utf8 -ErrorAction Stop
$h3QuoteExit = $LASTEXITCODE
$h3QuoteRecord.psql_exit_code = $h3QuoteExit
$h3QuoteRecord | ConvertTo-Json |
    Set-Content -LiteralPath (Join-Path $h3QuoteDir 'extraction.json') -Encoding utf8
if ($h3QuoteExit -ne 0) { throw "Quote extraction failed (exit $h3QuoteExit)" }
Move-Item -LiteralPath (Join-Path $h3QuoteDir 'observations.csv.partial') `
    -Destination (Join-Path $h3QuoteDir 'observations.csv') -ErrorAction Stop
python -m research.leader_risk.market_prices --observations $h3BaseCsv `
    --quotes (Join-Path $h3QuoteDir 'observations.csv') `
    --quote-extraction (Join-Path $h3QuoteDir 'extraction.json') `
    --output $h3QuoteDir --quote-max-age-ms 3000
if ($LASTEXITCODE -ne 0) { throw 'Quote validation or tabulation failed' }
```

The companion checks the extraction process, SQL hash, bounds, snapshot count
and exact market/checkpoint keys against the base export. It produces
`market_price_cells.csv` with all 160 cells, including the original and matched
loss counts/intervals, median bid and ask, quote availability and boundary-ask
counts. `price_bands.csv` recomputes risk separately within six non-overlapping
ask bands for each cell. Empty samples have unavailable rates and medians.
The report, coverage table and manifest retain missingness and provenance.

Prices and medians remain Decimal. Exact zero and one asks remain explicitly
identified boundary quotes. An ask below one is not by itself an executable
offer: tick rules, fees, delay, quantity and fills are outside this descriptive
addition. Its price-band comparisons do not inherit the full cell's loss rate
or establish profitability.

```powershell
python -m pytest tests/test_leader_risk_quote_extract.py tests/test_leader_risk_market_prices.py
```

## Reproduce the gross-return and exclusion audit

The `quote_return_audit` companion reads the same frozen base observations and
canonical quote CSV. It verifies their hashes, both snapshot clocks, the
extraction record, and the price companion's matching/helper hashes against
the original price manifest. It does not read new outcomes or run a new query.
Choose a new output directory; an existing directory is rejected.

```powershell
python -m research.leader_risk.quote_return_audit `
    --observations results/leader_risk/2026-09-12-full-history/observations.csv `
    --quotes results/leader_risk/2026-09-13-market-prices/observations.csv `
    --price-manifest results/leader_risk/2026-09-13-market-prices/manifest.json `
    --output results/leader_risk/example-return-audit
```

For each resolved matched observation, the gross benchmark is the recorded
leader's official binary payout minus its retained ask for one hypothetical
share. Price means use those same resolved observations; unknown outcomes
remain explicit and contribute neither an assumed payout nor a price to the
resolved mean. This is not future expected value, ROI, an observed fill, or net
profit. Boundary asks of zero and one stay separate, and the one-second
withdrawal limitation still applies.

Outputs include eight checkpoint totals, all 48 checkpoint/ask bands, all 960
cell/ask bands, full/matched/excluded coverage, five disjoint quote statuses,
and overlapping exclusion reasons. Diagnostic reasons distinguish source ages
strictly greater than 3,000 ms from future clocks and receipt staleness, and
isolate an unusable bid with a fresh ask from cases where both leader sides
are stale.

The `screens.csv` n/N >= 100 counts reproduce the review's selection rule.
`fixed_mean_ask_wilson_screen_lower` is only a diagnostic holding the observed
mean ask fixed; it is not a validated confidence bound for future paired
returns. The accompanying manifest records all inputs, helpers, code and
output hashes. The audit does not fit a model, select a trading strategy,
estimate fees, simulate execution, or activate the optional validation plan.

```powershell
python -m pytest tests/test_leader_risk_quote_return_audit.py
```
