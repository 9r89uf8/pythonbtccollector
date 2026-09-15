# Active reliability canary: completion handoff

Use only `C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-checkpoint-a-release`.
Preserve the unrelated dirty primary checkout and old canary evidence.

The owner authorized this exact one-hour live run. It started **2026-09-15
22:56:38 UTC** and ends **23:56:38 UTC** (5:56:38–6:56:38 PM CDT). The campaign
start is `1789512998000`, end `1789516598000`, runtime run ID
`fe2dd06da5754c50b696cb9419ca7251`, deployed launch commit `81ee6f1`.
`LAUNCH.json`, `PRELAUNCH_MANIFEST.json`, `EARLY_RUNNING.json` and the frozen
root protocol describe this run. All six horizons were healthy at the early
check; browser expiry calibration was valid and had no recorded errors.

## Identities and controls

- SSH: `root@152.42.247.86`; repo `/opt/price-collector`.
- State: `/var/lib/price-collector/ghost-reliability-20260915T225638Z`.
- Control: `/var/lib/price-collector/ghost-reliability-control-20260915T225638Z`.
- Redis observer: `/var/lib/price-collector/ghost-reliability-observer-20260915T225638Z`.
- Stop timer: `ghost-reliability-stop-20260915t225638z.timer`.
- Observer unit: `ghost-reliability-observer-20260915t225638z.service`.
- The timer executes the installed root-only `price_collector.ghost_twap_canary_stop`
  command in LAUNCH.json at the exact end. Its `stop.json` records request and
  completion clocks. It sets only this matching campaign's enabled flag false
  and restarts only Chainlink. API stays enabled. Do not extend or reactivate.

Before the deadline, make only brief read-only health/guard/observer checks.
Use new filenames for status snapshots, not overwrites of the early evidence.
Stay quiet while normal state is unchanged. Inspect sustained failures and stop
this exact campaign early if required; retain the actual partial run. Never
start a replacement campaign to fill missing time. Do not change capacity,
freshness, Redis/API bindings or retention policy.

## Browser and tunnel

The isolated Playwright CLI session is `ghost-reliability-canary`. The local SSH
tunnel is `127.0.0.1:19016` to droplet API9000; its known process ID is saved in
`output/playwright/reliability-canary/tunnel.pid`. Chrome runs headless with a
visible page and a 75-minute idle timeout. The probe captures one hour plus two
minutes from its own earlier start, without extending production. It stores raw
JSONL in browser memory until export, capped at128MiB/100k records.

Use the Playwright skill. The native Windows equivalent of its wrapper is:

```powershell
$env:npm_config_cache = 'C:/Users/alexa/PycharmProjects/polycollector/dist/ghost-checkpoint-a-release/dist/npm-cache'
npx.cmd --yes --package @playwright/cli playwright-cli -s=ghost-reliability-canary --raw run-code --filename=output/playwright/reliability-canary/browser_status.js
```

This session may require tool escalation for its own Playwright daemon/profile
directory. It never uses the user's personal browser profile. Do not reopen,
reload, replace the page, or re-run browser_probe.js over the running capture.
`run-code` has no `require`; use the supplied file scripts, not shell-quoted JS.
Campaign identity is already attached. Browser start, campaign start and the
first matching producer-object receipt are recorded separately.

After the capture's `done=true`, first run
`output/playwright/reliability-canary/export_preflight.ps1`, then invoke
`browser_export.js` through the same CLI. It downloads both `browser.jsonl` and
`campaign.json` into that directory once, refusing an earlier export attempt.
Preserve partial downloads and incomplete/capped captures instead of overwriting
or pretending they cover the hour. An end marker is not by itself completeness.

## Stop and export

Check completion after **23:59:38 UTC** to allow the bounded stop/drain and local
browser follow-through. The timer has already requested the stop at23:56:38;
do not wait120extra seconds before requesting it, as this run tests a pending tail.
Read its stop record and journal and confirm:

- Only the matching ghost producer is disabled; official feeds/API remain healthy.
- Current outbox is empty and every audit row is terminal (`incomplete_count=0`).
- Redis ghost key is absent; the observer manifest is complete and hash-valid.
- The three prior campaign hashes still match LAUNCH.json.
- Shutdown's drained/retained/late-event counts and actual duration are explicit.

Do not infer drainage from systemctl success. If incomplete, preserve records and
report the failure; do not guess targets or use archived recovery scripts.

Download the new full audit with the existing repo admin from this computer:

```powershell
& C:/Users/alexa/PycharmProjects/polycollector/.venv/Scripts/python.exe -m price_collector.ghost_twap_admin download --ssh root@152.42.247.86 --output dist/ghost-reliability-canary/audit.jsonl
```

It verifies the export and acknowledges unchanged row hashes. Keep the adjacent
manifest. Do not overwrite an existing final or `.part` file or start a duplicate
export while one is running. Preserve the earlier C export. Copy the observer
files to `dist/ghost-reliability-canary/observer`, verify their manifest, and
analyze them with `price_collector.ghost_twap_observer analyze` locally.

## Analysis and final record

The current C scripts are contract4-aware and take explicit paths/run identity:

- `research/spot_twap_response/checkpoint_c/analyze_browser.py`: supply the new
  capture and a new output directory, observation3600000/drain120000. Default
  warmup is capture+65s; either label it that way or use the frozen first verified
  same-campaign object+65s offset from browser metadata with `--warmup-ms`.
- `research/spot_twap_response/checkpoint_c/audit_check.py`: supply `--audit`,
  `--manifest`, `--browser`, `--run-id fe2dd06da5754c50b696cb9419ca7251`, `--output`.
  It checks exact browser/attempted producer bytes and membership. Never use its
  default old run ID.
- The archived combined-canary scorer remains v5/contract3-only. Do not run it
  unchanged on this campaign. Derive any additional contract4 coverage join
  locally, preserve Decimal financial arithmetic, and test changes meaningfully.

Report full-hour Redis bins separately from browser capture/overlap bins. Derive
campaign overlap using clock brackets, and a comparable accuracy slice ending
at least120s before actual stop; include uncertain boundaries and censored tail
targets. Audit receipt clocks are not browser receipt clocks. Do not claim a
live Redis reconnect or source-history retention unless one was observed.

Write `FINDINGS.md` and compact tables/manifests here, update the root canary
status, and push the official results to GitHub. Raw large audit/browser/observer
exports stay outside Git; do not expire records or change storage policy. Pull
the documentation/evidence commit on the droplet without restarting services.
Close only this Playwright session and its specifically identified SSH tunnel
after export; do not affect other browser/tunnel sessions. Pause the temporary
completion heartbeat after the work is finished and report the results.
