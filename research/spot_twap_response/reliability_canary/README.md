# Reliability canary audit

This offline scorer validates the completed export before selecting run
`fe2dd06da5754c50b696cb9419ca7251`. `_audit_v6.py` adapts the previous independent
combined-canary verifier for contract 4/runtime v6, with explicit reconnect
metadata and actual-publication expiry checks. It does not import forecast
calculation or runtime code. Test fixtures may use the runtime with fake I/O.

Financial calculations use Decimal precision 80 and ROUND_HALF_EVEN. Each
available forecast is checked against its exact 60 saved constituents. Error
basis points divide by the exact official target value. Quantiles interpolate
linearly at `(n - 1) * q`.

The primary panel retains decisions whose entire 120-second matching window
ends by the actual stop request, `1789516598428617010` wall nanoseconds. Separate
panels preserve all decisions, the stop tail, and a sensitivity panel starting
65 seconds after the scheduled campaign start. All calculated matches,
acknowledged eligible matches, and strictly early acknowledgements have separate
denominators. Missing, conflicted or invalid-clock targets are not price errors.

From the release checkout, using its normal Python environment:

```bash
python -m pytest research/spot_twap_response/reliability_canary/test_analyze.py -q
python -m research.spot_twap_response.reliability_canary.analyze --input dist/ghost-reliability-canary/audit.jsonl --run-id fe2dd06da5754c50b696cb9419ca7251 --campaign-start-ms 1789512998000 --stop-request-wall-ns 1789516598428617010 --expected-sha256 f104fa1507bc327254932faa52acf73432816867eb4257fceb5254b8e49d4954 --expected-rows 23411 --output results/spot_twap_response/2026-09-15-reliability-canary/audit_analysis --index-path dist/ghost-reliability-canary/audit_payload_index.json
python -m research.spot_twap_response.reliability_canary.join_observer --observer-directory dist/ghost-reliability-canary/observer --analysis-directory results/spot_twap_response/2026-09-15-reliability-canary/audit_analysis --launch results/spot_twap_response/2026-09-15-reliability-canary/LAUNCH.json --output results/spot_twap_response/2026-09-15-reliability-canary/observer_join.json
python -m pytest research/spot_twap_response/reliability_canary/test_observer_gaps.py -q
python -m research.spot_twap_response.reliability_canary.observer_gaps --observer-directory dist/ghost-reliability-canary/observer --output results/spot_twap_response/2026-09-15-reliability-canary/OBSERVER_GAPS.json
```

Both writers refuse to overwrite outputs. Choose new output paths for a repeat
run. The large input export and exact-payload index remain in ignored `dist/`;
the analysis manifest records their hashes.

The observer join revalidates ledger/sample hashes, byte classification and
read clocks, then requires the exact payload to exist in the accepted attempt
index. It compares exact target source stamps and both receipt clocks at read
end. Missing target receipts remain unknown; earlier saved receipts override
later per-decision matches. Same-host monotonic comparability rests on the
recorded launch/observer boot provenance. The audit export does not carry a
separate producer boot ID or prove complete feed capture.

`observer_gaps.py` separately reports consecutive known-absence, unknown and
per-horizon unusable bins, including post65 clipping. Unknown probes break
known-outage runs. A run's duration is its planned100ms-bin footprint, not proof
the cache remained absent between successful probes.

Decision records do not prove continuous cache coverage, browser delivery or
execution readiness. A missing reconnect counter is not proof no connection
changed. Redis subscriber reconnects require separate connection/log evidence.
