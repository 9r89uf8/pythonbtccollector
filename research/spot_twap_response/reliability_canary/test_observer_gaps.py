from research.spot_twap_response.reliability_canary.observer_gaps import summarize_runs


def test_panel_boundary_clips_runs_and_unknown_breaks_known_outage():
    # Positions2 and5 are unknown probes, not observed outages.
    known_absent = [True, True, False, True, True, False, True]
    result = summarize_runs(known_absent, 1, 7, 1000, 100)
    assert result['bins'] == 4 and result['runs'] == 3
    assert result['longest_consecutive_bins'] == 2
    longest, clipped, trailing = result['longest_runs']
    assert longest['first_bin'] == 3 and longest['last_bin_inclusive'] == 4
    assert longest['planned_bin_footprint_ms'] == 200
    assert longest['first_to_last_planned_probe_span_ms'] == 100
    assert clipped['first_bin'] == 1 and clipped['clipped_at_panel_start']
    assert clipped['planned_bin_footprint_ms'] == 100
    assert clipped['first_to_last_planned_probe_span_ms'] == 0
    assert trailing['first_bin'] == 6 and trailing['touches_panel_end']


def test_no_gap_and_full_panel_run():
    assert summarize_runs([False]*4, 0, 4, 0, 100)['longest_runs'] == []
    full = summarize_runs([True]*4, 0, 4, 0, 100)
    assert full['bins'] == 4 and full['longest_planned_bin_footprint_ms'] == 400
    run, = full['longest_runs']
    assert run['touches_panel_start'] and run['touches_panel_end']
    assert not run['clipped_at_panel_start']
