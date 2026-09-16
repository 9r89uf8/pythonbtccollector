# Reliability canary attribution review

Read-only local checks against the original `7c31c2a` results and saved raw
exports. These scripts do not connect to the droplet or alter original evidence.
Financial values remain Decimal strings; receipt/source clocks remain distinct.

- `check_windows_sleep.ps1` reproduces the original UTC-kind hashtable query,
  then compares corrected local bounds with explicit UTC XPath bounds. It keeps
  only the bounded relevant System events and sanitized data fields. The two
  corrected queries must return identical record IDs and in-range UTC stamps.
- `audit_target_corrections.py` streams and hashes the immutable full audit,
  checks the seven unpublished decisions, and inventories earlier missing
  stamps separately from continuation beyond the last observed TWAP stamp.
- `feed_gap_review.py` verifies the saved event index/observer evidence, checks
  contiguous offered-event sequences at the three gaps, reconstructs limiting
  expiry clocks and reports a conditional storage-capacity illustration.

Example Windows event reproduction from the release checkout, choosing an
unused destination:

```powershell
& ./research/spot_twap_response/reliability_review_corrections/check_windows_sleep.ps1 -OutputPath ./dist/windows-sleep-recheck.json
```

The Python tools expose their arguments with `--help`. Choose new output paths
for reruns; preserve the original evidence and existing outputs. Run focused
checks with:

```text
python -m pytest research/spot_twap_response/reliability_canary research/spot_twap_response/reliability_review_corrections -q
```

The original `FINAL_MANIFEST.json` still identifies documents at commit
`7c31c2a`. The new `peer_review_corrections/CORRECTION_MANIFEST.json` records
the revised document hashes and new evidence. No new accuracy score or missing
browser receipt is reconstructed by this review.
