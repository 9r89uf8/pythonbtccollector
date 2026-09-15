"""Exact parity on retained evidence, not a first-arrival or live-latency test."""
import csv
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

from price_collector.ghost_twap import GhostPolicy, GhostTwapEngine, PriceEvent


FIXTURE = Path(__file__).parent / "fixtures" / "ghost_twap" / "pending_v2"


def test_recorded_fixture_bytes_match_the_frozen_manifest():
    manifest = json.loads((FIXTURE / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "accepted"
    for filename, digest in manifest["artifacts_sha256"].items():
        assert hashlib.sha256((FIXTURE / filename).read_bytes()).hexdigest() == digest


def read_rows(name):
    with gzip.open(FIXTURE / name, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_recorded_hour_matches_independent_reference_on_all_21600_forecasts():
    source_rows = read_rows("recorded_hour_events.csv.gz")
    decisions = read_rows("recorded_hour_decisions.csv.gz")
    expected = read_rows("recorded_hour_expected.csv.gz")
    assert len(source_rows) == 7505
    assert len(decisions) == 3600
    assert len(expected) == 21600
    events = [PriceEvent(
        feed=row["kind"], value=Decimal(row["price"]),
        source_timestamp_ms=int(row["source_ms"]),
        received_wall_ns=int(row["replay_received_wall_ns"]),
        # Synthetic order clock declared in fixture manifest; no live delay claim.
        received_monotonic_ns=int(row["replay_monotonic_ns"]),
        sequence=int(row["replay_order"]), event_id=row["event_id"],
        window_s=60 if row["kind"] == "twap" else None,
    ) for row in source_rows]
    # The historical fixture retains its original 3s/3s freshness policy.
    engine = GhostTwapEngine("recorded-hour", GhostPolicy(
        enabled=True, source_max_age_ms=3000, receipt_max_age_ms=3000,
        spot_reconnect_max_gap_ms=0))
    next_event = 0
    checked = 0
    available = {h: 0 for h in (1, 2, 3, 5, 10, 30)}
    expected_by_key = {(row["decision_id"], int(row["horizon_s"])): row for row in expected}
    assert len(expected_by_key) == len(expected)
    for cutoff in decisions:
        wall = int(cutoff["decision_wall_ns"])
        while next_event < len(events) and events[next_event].received_wall_ns <= wall:
            engine.accept(events[next_event])
            next_event += 1
        decision = engine.snapshot(cutoff["decision_id"], wall, int(cutoff["decision_monotonic_ns"]))
        assert len(decision.slots) == 89
        assert engine.history_size <= engine.policy.max_events
        for forecast in decision.forecasts:
            row = expected_by_key[(decision.decision_id, forecast.horizon_s)]
            clue = (decision.decision_id, forecast.horizon_s)
            assert decision.current_spot.event_id == row["current_spot_event_id"], clue
            assert decision.current_twap.event_id == row["current_twap_event_id"], clue
            assert decision.included_sequence == int(row["included_sequence"]), clue
            assert forecast.target_source_timestamp_ms == int(row["target_source_timestamp_ms"]), clue
            assert forecast.price == (Decimal(row["price"]) if row["price"] else None), clue
            assert forecast.quality == row["quality"], clue
            assert set(forecast.reasons) == set(filter(None, row["reasons"].split("|"))), clue
            assert (forecast.counts.observed, forecast.counts.carried,
                    forecast.counts.pending, forecast.counts.future, forecast.counts.missing) == tuple(
                        int(row[key]) for key in ("exact", "carried", "pending", "future", "missing")), clue
            assert sum(vars(forecast.counts).values()) == 60, clue
            assert forecast.estimated_arrival_wall_ns == int(row["estimated_arrival_wall_ns"]), clue
            assert forecast.estimated_remaining_ns == int(row["estimated_remaining_ns"]), clue
            assert forecast.estimate_overdue == (row["estimate_overdue"].lower() in ("true", "1")), clue
            available[forecast.horizon_s] += forecast.price is not None
            checked += 1
    assert checked == 21600
    assert set(available.values()) == {3585}
