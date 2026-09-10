import asyncio
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
import json
from pathlib import Path
import time
from uuid import UUID, uuid4

import pytest

from price_collector import polymarket_evidence_store as store


CONNECTION = UUID("1da6d988-dfcf-4ebf-b7e8-75675497973f")
WALL_NS = 1_789_034_580_000_000_000
MONO_NS = 9_000_000_000
MARKET_ID = (WALL_NS // 1_000_000) // 300_000


def evidence(**overrides):
    values = dict(record_id=uuid4(), market_id=MARKET_ID, kind="price_to_beat",
                  received_wall_ns=WALL_NS, received_monotonic_ns=MONO_NS,
                  payload={"price_to_beat": Decimal("64255.113422936400000001")})
    values.update(overrides)
    return store.EvidenceRecord(**values)


def quote(**overrides):
    values = dict(connection_id=CONNECTION, market_id=MARKET_ID,
                  observed_wall_ns=WALL_NS + 100_000_000,
                  observed_monotonic_ns=MONO_NS + 100_000_000,
                  receive_sequence=10, received_wall_ns=WALL_NS,
                  received_monotonic_ns=MONO_NS,
                  up_bid=Decimal("0.80"), up_ask=Decimal("0.81"),
                  down_bid=Decimal("0.19"), down_ask=Decimal("0.20"),
                  up_bid_provider_event_ms=WALL_NS // 1_000_000 - 3,
                  up_ask_provider_event_ms=WALL_NS // 1_000_000 - 4,
                  down_bid_provider_event_ms=WALL_NS // 1_000_000 - 5,
                  down_ask_provider_event_ms=WALL_NS // 1_000_000 - 6,
                  up_bid_received_ms=WALL_NS // 1_000_000,
                  up_ask_received_ms=WALL_NS // 1_000_000 - 1,
                  down_bid_received_ms=WALL_NS // 1_000_000 - 2,
                  down_ask_received_ms=WALL_NS // 1_000_000 - 3,
                  event_type="best_bid_ask")
    values.update(overrides)
    return store.QuoteObservation(**values)


class Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        return False


class MemoryPool:
    """Small database double that applies the actual insert argument contracts."""
    def __init__(self):
        self.payloads = {}
        self.observations = {}
        self.quotes = {}
        self.market_ids = set()
        self.size_bytes = 0
        self.statements = []
        self.operations = []

    def acquire(self):
        return Context(self)

    def transaction(self):
        return Context(self)

    async def execute(self, sql):
        self.statements.append(sql)

    async def executemany(self, sql, rows):
        self.statements.append(sql)
        for row in rows:
            if "INSERT INTO market_windows" in sql:
                self.market_ids.add(row[0])
            elif "INSERT INTO polymarket_evidence_payloads" in sql:
                self.payloads.setdefault(row[0], json.loads(row[1]))
            elif "INSERT INTO polymarket_market_observations" in sql:
                same_session_kind = any(
                    existing[9] == row[9] and existing[2] == row[2]
                    for existing in self.observations.values()
                ) if row[2] in {"session_start", "session_end"} else False
                if not same_session_kind:
                    self.observations.setdefault(row[0], row)
                self.operations.append(("metadata", row[2], row[0]))
            elif "INSERT INTO polymarket_quote_observations" in sql:
                self.quotes.setdefault((row[0], row[2]), row)
                self.operations.append(("quote", row[0], row[2]))
            else:
                raise AssertionError(sql)

    async def fetchval(self, sql):
        self.statements.append(sql)
        return self.size_bytes

    async def fetch(self, sql, *args):
        self.statements.append(sql)
        if "WITH candidates AS" in sql:
            connections = sorted({row[0] for row in self.quotes.values()})
            if args:
                connections = [connection_id for connection_id in connections if connection_id > args[0]]
            scanned = []
            for connection_id in connections[:128]:
                latest = max((row for row in self.quotes.values() if row[0] == connection_id),
                             key=lambda row: row[2])
                scanned.append(dict(
                    connection_id=connection_id, market_id=latest[1], session_start_wall_ns=None,
                    last_durable_observed_wall_ns=latest[2], last_durable_received_wall_ns=latest[5],
                    last_durable_receive_sequence=latest[4],
                    session_recorded=any(row[9] == connection_id and row[2] in {"session_start", "session_end"}
                                         for row in self.observations.values()),
                ))
            return scanned
        recovered = []
        for row in self.observations.values():
            if row[2] != "session_start":
                continue
            if any(end[2] == "session_end" and end[9] == row[9]
                   for end in self.observations.values()):
                continue
            matching_quotes = [q for q in self.quotes.values() if q[0] == row[9]]
            latest = max(matching_quotes, key=lambda q: q[2]) if matching_quotes else None
            recovered.append(dict(
                connection_id=row[9], market_id=row[1], session_start_wall_ns=row[3],
                last_durable_observed_wall_ns=latest[2] if latest else None,
                last_durable_received_wall_ns=latest[5] if latest else None,
                last_durable_receive_sequence=latest[4] if latest else None,
            ))
        return recovered[:128]


async def eventually(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.005)


def test_payload_is_exact_canonical_deduplicated_and_deeply_immutable():
    original = {"nested": [{"fee": Decimal("0.0700")}], "strike": Decimal("64255.113422936400000001")}
    first = evidence(payload=original)
    second = evidence(payload={"strike": original["strike"], "nested": original["nested"]})
    assert first.payload_hash == second.payload_hash
    assert json.loads(first.payload_json)["strike"] == "64255.113422936400000001"
    assert json.loads(first.payload_json)["nested"][0]["fee"] == "0.0700"
    original["nested"][0]["fee"] = Decimal("9")
    assert first.payload["nested"][0]["fee"] == "0.0700"
    with pytest.raises(TypeError):
        first.payload["nested"][0]["fee"] = "1"
    with pytest.raises(FrozenInstanceError):
        first.status = "missing"


@pytest.mark.parametrize("payload", [
    {"strike": 0.1}, {"nested": [{"fee": 0.07}]},
    {"strike": Decimal("NaN")}, {"strike": Decimal("Infinity")}, {1: "wrong-key"},
])
def test_payload_rejects_float_nonfinite_and_nonstring_keys(payload):
    with pytest.raises((TypeError, ValueError)):
        evidence(payload=payload)


def test_record_keeps_wall_clock_reversal_but_rejects_monotonic_reversal():
    row = evidence(requested_wall_ns=WALL_NS + 1,
                   requested_monotonic_ns=MONO_NS - 1)
    assert row.requested_wall_ns > row.received_wall_ns
    with pytest.raises(ValueError, match="monotonic"):
        evidence(requested_wall_ns=WALL_NS, requested_monotonic_ns=MONO_NS + 1)
    with pytest.raises(ValueError, match="together"):
        evidence(requested_wall_ns=WALL_NS)


def test_response_provenance_is_typed_and_does_not_defeat_payload_deduplication():
    async def run():
        first = evidence(response_sha256="a" * 64, response_date="Thu, 10 Sep 2026 03:00:00 GMT",
                         response_age_seconds=0)
        second = evidence(response_sha256="b" * 64, response_date="Thu, 10 Sep 2026 03:00:05 GMT",
                          response_age_seconds=5)
        assert first.payload_hash == second.payload_hash
        pool = MemoryPool()
        await store.EvidenceWriter(pool)._persist_metadata([first, second])
        assert len(pool.payloads) == 1
        assert pool.observations[first.record_id][12:] == (
            first.response_sha256, first.response_date, first.response_age_seconds)
    asyncio.run(run())
    for kwargs in ({"response_sha256": "not-a-hash"}, {"response_age_seconds": -1},
                   {"response_age_seconds": True}, {"response_date": 123}):
        with pytest.raises((TypeError, ValueError)):
            evidence(**kwargs)


@pytest.mark.parametrize("overrides", [
    {"up_ask": 0.81}, {"up_ask": Decimal("1.1")},
    {"receive_sequence": -1}, {"received_wall_ns": None},
    {"observed_monotonic_ns": MONO_NS - 1},
    {"up_ask_received_ms": -1}, {"resolved": "false"},
    {"received_wall_ns": store.MAX_BIGINT + 1},
    {"up_ask": Decimal("0.8000000000000000001")},
])
def test_quote_rejects_invalid_values_before_they_can_poison_writer_queue(overrides):
    with pytest.raises((TypeError, ValueError)):
        quote(**overrides)


def test_quote_preserves_actual_observation_and_original_component_clocks():
    row = quote(observed_wall_ns=WALL_NS + 147_999_999,
                observed_monotonic_ns=MONO_NS + 147_999_999)
    assert row.observed_wall_ns % 100_000_000 == 47_999_999
    assert row.up_ask_received_ms != row.down_ask_received_ms
    assert row.received_wall_ns < row.observed_wall_ns
    initial = quote(receive_sequence=0, received_wall_ns=None, received_monotonic_ns=None,
                    up_bid=None, up_ask=None, down_bid=None, down_ask=None,
                    **{key: None for key in store.QUOTE_COMPONENT_CLOCKS})
    assert initial.received_wall_ns is None
    assert initial.up_ask is None
    assert quote(up_ask=Decimal("0.800000000000000001")).up_ask == Decimal("0.800000000000000001")


def test_metadata_bigint_fields_are_rejected_before_sql():
    for name in ("received_wall_ns", "provider_event_ms", "response_age_seconds"):
        with pytest.raises(ValueError):
            evidence(**{name: store.MAX_BIGINT + 1})
    assert evidence(kind="clob_order_rules").kind == "clob_order_rules"


def test_idempotent_append_and_payload_deduplication_preserve_revisions():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool)
        first = evidence()
        repeated_poll = replace(first, record_id=uuid4(), received_wall_ns=WALL_NS + 1)
        revised = evidence(payload={"price_to_beat": Decimal("64255.113422936400000002")})
        await writer._persist_metadata([first, repeated_poll, revised])
        await writer._persist_metadata([first, repeated_poll, revised])
        assert len(pool.observations) == 3
        assert len(pool.payloads) == 2
        assert pool.observations[first.record_id][3] == WALL_NS
        assert all("DO UPDATE" not in sql for sql in pool.statements)
        old = quote()
        refresh = replace(old, observed_wall_ns=old.observed_wall_ns + 100_000_000,
                          observed_monotonic_ns=old.observed_monotonic_ns + 100_000_000,
                          up_ask_received_ms=old.up_ask_received_ms + 1)
        await writer._persist_quotes([old, refresh, old])
        assert len(pool.quotes) == 2
        assert all(isinstance(row[8], Decimal) for row in pool.quotes.values())
        assert pool.market_ids == {MARKET_ID}
    asyncio.run(run())


def test_overflow_preserves_queued_rows_and_aggregates_durable_scoped_gaps():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, queue_max=1, quote_queue_max=1, flush_seconds=0.001)
        await writer._check_size()
        first = evidence()
        assert writer.offer(first)
        assert not writer.offer(evidence(received_wall_ns=WALL_NS + 1))
        assert not writer.offer(evidence(received_wall_ns=WALL_NS + 2))
        first_quote = quote()
        assert writer.offer_quote(first_quote)
        assert not writer.offer_quote(replace(first_quote, observed_wall_ns=WALL_NS + 200_000_000))
        assert not writer.offer_quote(replace(first_quote, observed_wall_ns=WALL_NS + 300_000_000))
        # Metadata pressure cannot exhaust the separately reserved session/control queue.
        assert writer.offer(evidence(kind="session_start", connection_id=CONNECTION))
        assert writer._metadata_queue.qsize() == writer._quote_queue.qsize() == 1
        await writer.close()
        assert first.record_id in pool.observations
        assert len(pool.quotes) == 1
        gaps = [pool.payloads[row[11]] for row in pool.observations.values() if row[2] == "gap"]
        assert len(gaps) == 2
        assert {gap["channel"] for gap in gaps} == {"metadata", "quotes"}
        assert all(gap["lost_observations"] == 2 for gap in gaps)
        quote_gap = next(gap for gap in gaps if gap["channel"] == "quotes")
        assert quote_gap["first_lost_wall_ns"] == WALL_NS + 200_000_000
        assert quote_gap["last_lost_wall_ns"] == WALL_NS + 300_000_000
    asyncio.run(run())


def test_loss_scope_memory_is_bounded_and_excess_scope_is_explicit():
    async def run():
        writer = store.EvidenceWriter(MemoryPool())
        for index in range(store.MAX_LOSS_SCOPES + 200):
            assert not writer.offer_quote(quote(market_id=MARKET_ID + index,
                                               connection_id=uuid4()))
        assert len(writer._losses) == store.MAX_LOSS_SCOPES
        assert writer._overflow_loss.count == 200
        payload = writer._overflow_loss.as_record().payload
        assert payload["scope_overflow"] is True
        assert payload["first_market_id"] == MARKET_ID + store.MAX_LOSS_SCOPES
        assert payload["last_market_id"] == MARKET_ID + store.MAX_LOSS_SCOPES + 199
    asyncio.run(run())


def test_full_metadata_batch_includes_ending_session_losses_atomically():
    async def run():
        writer = store.EvidenceWriter(MemoryPool(), batch_size=2)
        writer._next_loss_flush_at = time.monotonic() + 60
        assert not writer.offer_quote(quote())
        end = evidence(kind="session_end", connection_id=CONNECTION)
        ordinary = evidence()
        assert writer.offer(end)
        assert writer.offer(ordinary)
        records, _queues = writer._metadata_batch()
        # Bounded correlated gap controls may exceed the nominal batch size.
        assert len(records) == 3
        assert {record.kind for record in records} == {"gap", "session_end", "price_to_beat"}
        assert not writer._losses
    asyncio.run(run())


def test_full_deferred_end_batch_cannot_bypass_atomic_loss_flush():
    async def run():
        writer = store.EvidenceWriter(MemoryPool(), batch_size=1)
        writer._next_loss_flush_at = time.monotonic() + 60
        assert not writer.offer_quote(quote())
        end = evidence(kind="session_end", connection_id=CONNECTION)
        writer._deferred_ends[CONNECTION] = end
        records, _queues = writer._metadata_batch()
        assert len(records) == 2
        assert {record.kind for record in records} == {"session_end", "gap"}
        assert not writer._losses
    asyncio.run(run())


def test_sustained_quote_pause_coalesces_gaps_between_periodic_flushes():
    async def run():
        writer = store.EvidenceWriter(MemoryPool())
        assert not writer.offer_quote(quote())
        first, _queues = writer._metadata_batch()
        assert len(first) == 1 and first[0].kind == "gap"
        for _ in range(100):
            assert not writer.offer_quote(quote())
        pending, _queues = writer._metadata_batch()
        assert pending == []
        assert next(iter(writer._losses.values())).count == 100
        writer._closing_event.set()
        final, _queues = writer._metadata_batch()
        assert final[0].payload["lost_observations"] == 100
    asyncio.run(run())


def test_quote_size_guard_preserves_backlog_and_metadata_and_uses_all_new_relations():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, max_relation_mb=2, warn_relation_mb=1,
                                      flush_seconds=0.001)
        await writer._check_size()
        assert writer.offer_quote(quote())
        pool.size_bytes = 2 * 1024 * 1024
        await writer._check_size()
        assert writer.quote_writes_paused
        assert not writer.offer_quote(quote(observed_wall_ns=WALL_NS + 200_000_000))
        metadata = evidence()
        assert writer.offer(metadata)
        pool.size_bytes = 1 * 1024 * 1024
        await writer._check_size()
        assert writer.quote_writes_paused  # hysteresis until below warning
        await writer.close()
        assert len(pool.quotes) == 1  # a new-offer guard must not discard accepted history
        assert metadata.record_id in pool.observations
        assert any(row[2] == "gap" for row in pool.observations.values())
        assert "pg_total_relation_size" in store._SIZE_SQL
        for table in ("polymarket_evidence_payloads", "polymarket_market_observations",
                      "polymarket_quote_observations"):
            assert table in store._SIZE_SQL
        assert "raw_capture" not in store._SIZE_SQL
        assert all("DELETE " not in sql and "DROP " not in sql for sql in pool.statements)
    asyncio.run(run())


def test_size_check_failure_pauses_quotes_without_stopping_metadata(monkeypatch):
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, flush_seconds=0.001)
        async def broken_size():
            raise ConnectionError("cannot measure")
        monkeypatch.setattr(writer, "_check_size", broken_size)
        await writer.start()
        await eventually(lambda: writer._pause_reason == "relation_size_check_failed")
        assert not writer.offer_quote(quote())
        row = evidence()
        assert writer.offer(row)
        await eventually(lambda: row.record_id in pool.observations)
        await writer.close()
    asyncio.run(run())


def test_slow_quote_write_does_not_block_metadata_and_session_end_waits(monkeypatch):
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, flush_seconds=0.001)
        quote_started, release_quote = asyncio.Event(), asyncio.Event()
        original_write = writer._persist_quotes
        async def blocked_quotes(records):
            quote_started.set()
            await release_quote.wait()
            await original_write(records)
        monkeypatch.setattr(writer, "_persist_quotes", blocked_quotes)
        await writer.start()
        await eventually(lambda: not writer.quote_writes_paused)
        start = evidence(kind="session_start", connection_id=CONNECTION)
        end = evidence(kind="session_end", connection_id=CONNECTION)
        row = evidence()
        assert writer.offer(start)
        assert writer.offer_quote(quote())
        await quote_started.wait()
        assert writer.offer(end)
        assert writer.offer(row)
        await eventually(lambda: row.record_id in pool.observations)
        assert start.record_id in pool.observations
        assert end.record_id not in pool.observations
        release_quote.set()
        await eventually(lambda: end.record_id in pool.observations)
        await writer.close()
        quote_index = next(i for i, op in enumerate(pool.operations) if op[0] == "quote")
        end_index = next(i for i, op in enumerate(pool.operations)
                         if op[0] == "metadata" and op[1] == "session_end")
        assert quote_index < end_index
    asyncio.run(run())


def test_session_end_cannot_pass_accepted_connection_metadata():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, batch_size=1, flush_seconds=0.001)
        tick = evidence(kind="tick_size", connection_id=CONNECTION,
                        payload={"tick_size": Decimal("0.001")})
        end = evidence(kind="session_end", connection_id=CONNECTION)
        assert writer.offer(tick)
        assert writer.offer(end)
        await writer.close()
        tick_index = next(i for i, op in enumerate(pool.operations)
                          if op[0] == "metadata" and op[1] == "tick_size")
        end_index = next(i for i, op in enumerate(pool.operations)
                         if op[0] == "metadata" and op[1] == "session_end")
        assert tick_index < end_index
        assert not writer._pending_metadata
    asyncio.run(run())


def test_retry_after_uncertain_commit_is_idempotent(monkeypatch):
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, flush_seconds=0.001)
        original = writer._persist_metadata
        attempts = []
        async def uncertain_commit(records):
            await original(records)
            attempts.append(tuple(record.record_id for record in records))
            if len(attempts) == 1:
                raise ConnectionError("commit acknowledgement lost")
        monkeypatch.setattr(writer, "_persist_metadata", uncertain_commit)
        row = evidence()
        writer.offer(row)
        await writer.start()
        await eventually(lambda: len(attempts) >= 2)
        await writer.close()
        assert attempts[0] == attempts[1]
        assert len(pool.observations) == len(pool.payloads) == 1
    asyncio.run(run())


def test_recovery_records_last_durable_quote_and_unknown_disconnect_idempotently():
    async def run():
        pool = MemoryPool()
        original = store.EvidenceWriter(pool)
        await original._persist_metadata([evidence(kind="session_start", connection_id=CONNECTION)])
        last = quote()
        await original._persist_quotes([last])
        recovery = store.EvidenceWriter(pool)
        await recovery._recover_sessions()
        await recovery._recover_sessions()
        assert len(pool.observations) == 3
        gaps = [row for row in pool.observations.values() if row[2] == "gap"]
        assert len(gaps) == 1
        payload = pool.payloads[gaps[0][11]]
        assert payload["disconnect_time_known"] is False
        assert payload["last_durable_observed_wall_ns"] == last.observed_wall_ns
        assert payload["last_durable_received_wall_ns"] == last.received_wall_ns
        assert payload["last_durable_receive_sequence"] == last.receive_sequence
        assert payload["detected_wall_ns"] == gaps[0][3]
        assert payload["detected_wall_ns"] != last.observed_wall_ns
        assert not await pool.fetch(store._RECOVERY_SQL)
    asyncio.run(run())


def test_recovery_does_not_invent_last_receipt_for_an_empty_session():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool)
        await writer._persist_metadata([evidence(kind="session_start", connection_id=CONNECTION)])
        await writer._recover_sessions()
        gap = next(row for row in pool.observations.values() if row[2] == "gap")
        payload = pool.payloads[gap[11]]
        assert payload["last_durable_observed_wall_ns"] is None
        assert payload["last_durable_received_wall_ns"] is None
    asyncio.run(run())


def test_recovery_finds_orphan_quotes_in_bounded_pages_without_inventing_start():
    async def run():
        pool = MemoryPool()
        old = store.EvidenceWriter(pool)
        # Force a second candidate page; the independent quote writer may have
        # committed these while no session_start was durable.
        rows = [quote(connection_id=UUID(int=index + 1)) for index in range(130)]
        await old._persist_quotes(rows)
        recovery = store.EvidenceWriter(pool)
        await recovery._recover_sessions()
        await recovery._recover_sessions()
        gaps = [row for row in pool.observations.values() if row[2] == "gap"]
        assert len(gaps) == 130
        assert len(pool.observations) == 260
        assert all(pool.payloads[row[11]]["missing_start_record"] for row in gaps)
        assert all(pool.payloads[row[11]]["session_start_wall_ns"] is None for row in gaps)
        assert store._ORPHAN_SCAN_NEXT_SQL in pool.statements
    asyncio.run(run())


def test_recovery_does_not_close_a_live_connection_started_by_this_writer():
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool)
        await writer._check_size()
        row = quote()
        assert writer.offer_quote(row)
        await writer._persist_quotes([row])
        await writer._recover_sessions()
        assert not pool.observations
        await writer.close()
    asyncio.run(run())


def test_start_and_shutdown_are_bounded_when_database_hangs(monkeypatch, caplog):
    async def run():
        pool = MemoryPool()
        writer = store.EvidenceWriter(pool, flush_seconds=0.001)
        async def hanging_recovery():
            await asyncio.Event().wait()
        monkeypatch.setattr(writer, "_recover_sessions", hanging_recovery)
        started = time.monotonic()
        await writer.start()
        assert time.monotonic() - started < 0.1
        writer.offer(evidence())
        await writer.close(timeout_seconds=0.05)
        assert time.monotonic() - started < 0.5
        assert all(task.done() for task in writer._tasks)
        assert "polymarket_evidence_shutdown_incomplete" in caplog.text
    asyncio.run(run())


def test_shutdown_does_not_wait_forever_for_slow_driver_cancellation(monkeypatch, caplog):
    async def run():
        writer = store.EvidenceWriter(MemoryPool(), flush_seconds=0.001)
        cancelling = asyncio.Event()
        release = asyncio.Event()
        async def slow_cancel_recovery():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelling.set()
                await release.wait()
        monkeypatch.setattr(writer, "_recover_sessions", slow_cancel_recovery)
        await writer.start()
        started = time.monotonic()
        await writer.close(timeout_seconds=0.02)
        assert time.monotonic() - started < 0.2
        assert cancelling.is_set()
        assert "polymarket_evidence_shutdown_tasks_still_running" in caplog.text
        release.set()
        await asyncio.gather(*writer._tasks, return_exceptions=True)
    asyncio.run(run())


def test_sql_timeout_covers_pool_acquisition_and_transaction(monkeypatch):
    class HangingAcquire:
        async def __aenter__(self):
            await asyncio.Event().wait()
        async def __aexit__(self, *args):
            return False
    class HangingPool:
        def acquire(self):
            return HangingAcquire()
    async def run():
        writer = store.EvidenceWriter(HangingPool())
        monkeypatch.setattr(store, "SQL_TIMEOUT_SECONDS", 0.02)
        with pytest.raises(asyncio.TimeoutError):
            await writer._persist_metadata([evidence()])
    asyncio.run(run())


def test_schema_has_append_only_keys_causal_clocks_and_reader_grants():
    schema = (Path(__file__).resolve().parents[1] / "schema.sql").read_text()
    def table(name):
        start = schema.index(f"CREATE TABLE IF NOT EXISTS {name}")
        return schema[start:schema.index(";", start)]
    payloads = table("polymarket_evidence_payloads")
    metadata = table("polymarket_market_observations")
    quotes = table("polymarket_quote_observations")
    assert "payload_hash TEXT PRIMARY KEY" in payloads
    assert "record_id UUID PRIMARY KEY" in metadata
    assert "REFERENCES polymarket_evidence_payloads(payload_hash)" in metadata
    assert "PRIMARY KEY (connection_id, observed_wall_ns)" in quotes
    assert "ON polymarket_quote_observations (market_id, observed_wall_ns)" in schema
    assert "ON polymarket_market_observations (connection_id, kind)" in schema
    assert "requested_monotonic_ns <= received_monotonic_ns" in metadata
    assert "received_monotonic_ns <= observed_monotonic_ns" in quotes
    assert "(received_wall_ns IS NULL) = (received_monotonic_ns IS NULL)" in quotes
    assert "receive_sequence >= 0" in quotes
    assert "up_ask NUMERIC(38, 18)" in quotes
    assert "'clob_order_rules'" in metadata
    assert "raw JSONB" not in quotes
    for name in store.QUOTE_COMPONENT_CLOCKS:
        assert f"{name} BIGINT" in quotes
    for name in ("polymarket_evidence_payloads", "polymarket_market_observations",
                 "polymarket_quote_observations"):
        assert f"GRANT SELECT, INSERT ON {name} TO price_writer" in schema
        assert f"GRANT SELECT ON {name} TO price_reader" in schema
        assert f"GRANT SELECT, INSERT, UPDATE ON {name}" not in schema
        assert schema.index(f"REVOKE UPDATE, DELETE ON {name} FROM price_writer") > schema.index(
            "ON ALL TABLES IN SCHEMA public TO price_writer")
