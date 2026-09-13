# Checkpoint A recorded replay preparation

Contract 2 adds a separate `pending` count for usable nonexact historical slots
beyond the maximum admissible spot source timestamp in the received prefix.
Interior nonexact slots remain `carried`. This changes labels only: all 21,600
prices, source identifiers, receipt cutoffs, availability reasons, targets, and
signed arrival estimates match the original frozen CSV exactly.

`build_fixture_pending.py` writes a complete new fixture under
[`tests/fixtures/ghost_twap/pending_v2/`](../../../tests/fixtures/ghost_twap/pending_v2/README.md)
and records the old/new quality table and immutable-field checks in
[`pending_review.json`](pending_review.json). Every row satisfies
`original carried = new carried + pending`, with five categories totaling 60.
Only interior carry degrades an otherwise available forecast; pending and
future counts remain visible assumptions.

The original `build_fixture.py`, three original gzip files, manifest, README,
and validation record remain untouched. The event and decision files in the new
fixture are byte-identical to the originals. To rebuild contract 2 into fresh
locations:

```powershell
python research/spot_twap_response/checkpoint_a_replay/build_fixture_pending.py --output NEW_LOCAL_DIRECTORY --review-output NEW_REVIEW_FILE.json
```

Both commands refuse to overwrite their outputs. The contract 2 manifest hashes
its generator and the preserved contract 1 generator and artifacts. The labels
do not prove that a pending exact report will arrive or that an interior absent
stamp was a silent feed; all retained-source and synthetic-clock limitations
below remain applicable.

## Original contract 1 preparation

`build_fixture.py` uses only the already retained
`research/spot_twap_response/pilot_review/sources.csv` export and its original
extraction record. It validates source and query hashes, preserves the original
seven CSV fields, selects the predetermined September 11 hour, and produces the
fixture and an independent expected-calculation table. It imports no production
module and makes no network or database calls.

The accepted files and their schema, counts, and limitations are documented in
[`tests/fixtures/ghost_twap/README.md`](../../../tests/fixtures/ghost_twap/README.md).
Its manifest hashes the source, extraction record, original SQL, generator, and
three compressed CSV artifacts.

To reproduce into a new directory from the repository root:

```powershell
python research/spot_twap_response/checkpoint_a_replay/build_fixture.py --output NEW_LOCAL_DIRECTORY
```

The default output is `tests/fixtures/ghost_twap`. The command refuses to
overwrite existing fixture artifacts. A rebuild requires the original retained
source export, which is not embedded in the compact fixture. Engine replay tests
can consume the compact fixture without that larger source file.

All 3,600 one-second cutoffs and six horizons are retained, including the 15
unavailable cutoffs. No forecast-error or quality threshold selected the hour.
The expected calculations use only the available receipt prefix, not realized
future TWAP. The fixture's monotonic clock and acceptance sequence are explicitly
synthetic replay conveniences; the original export did not retain those clocks.
