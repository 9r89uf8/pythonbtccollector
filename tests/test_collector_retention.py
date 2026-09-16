"""Focused ten-day policy checks and one opt-in disposable PostgreSQL lifecycle."""
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from price_collector.retention_policy import HISTORY_RETENTION_MS, retained_market_floor_ms

DSN = os.environ.get("GHOST_TEST_POSTGRES_DSN")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RETIRED = ("binance_microstructure_1s_flip_archive", "chainlink_twap_shadow_predictions",
           "polymarket_btc_5m_flip_evaluations", "polymarket_btc_5m_flip_cutoffs", "polymarket_btc_5m_flip_events")


def test_market_floor_rounds_ten_day_cutoff_up_and_rejects_bad_clocks():
    boundary = 1_800_000_000_000
    assert HISTORY_RETENTION_MS == 10*86_400_000
    assert retained_market_floor_ms(boundary+HISTORY_RETENTION_MS) == boundary
    assert retained_market_floor_ms(boundary+HISTORY_RETENTION_MS+1) == boundary+300_000
    assert retained_market_floor_ms(0) == 0
    for bad in (True, -1, 1.5):
        with pytest.raises(ValueError):
            retained_market_floor_ms(bad)


def test_cli_default_only_inspects(monkeypatch, capsys):
    from price_collector import retention
    calls = []
    class Connection:
        async def fetchval(self, query):
            return 1_800_000_000_000
        async def close(self):
            calls.append("closed")
    async def connect(**kwargs):
        assert kwargs["host"] == "/var/run/postgresql" and kwargs["user"] == "postgres"
        return Connection()
    async def inspect(connection, now_ms):
        calls.append("inspected")
        return {"remaining":{"public.price_samples":True}}
    async def forbidden(*args, **kwargs):
        raise AssertionError("default CLI must not delete")
    monkeypatch.setattr(retention.asyncpg,"connect",connect)
    monkeypatch.setattr(retention,"inspect_history",inspect)
    monkeypatch.setattr(retention,"expire_history",forbidden)
    monkeypatch.setattr("sys.argv",["collector-retention"])
    with pytest.raises(SystemExit) as exit_status:
        retention.main()
    assert exit_status.value.code == 0 and calls == ["inspected","closed"]
    assert json.loads(capsys.readouterr().out)["remaining"]["public.price_samples"] is True


async def insert(connection, table, **values):
    # Identifiers are fixture constants; values always use bind parameters.
    assert table.replace("_", "").replace(".", "").isalnum()
    columns = ",".join(values)
    slots = ",".join(f"${i}" for i in range(1,len(values)+1))
    await connection.execute(f"INSERT INTO {table} ({columns}) VALUES ({slots})", *values.values())


async def seed_market(connection, start, instrument, session, sequence, payload):
    market_id = start//300_000
    at = EPOCH+timedelta(milliseconds=start)
    await insert(connection,"market_windows",market_id=market_id,market_start_ms=start,market_end_ms=start+300_000,
                 market_start_at=at,market_end_at=at+timedelta(minutes=5))
    await insert(connection,"polymarket_btc_5m_markets",market_id=market_id,slug=f"retention-{market_id}",
                 up_token_id=f"up-{market_id}",down_token_id=f"down-{market_id}",first_seen_ms=start,last_seen_ms=start)
    await insert(connection,"polymarket_btc_5m_resolutions",market_id=market_id,first_checked_ms=start,last_checked_ms=start)
    sample = dict(market_id=market_id,sample_second_ms=start,sample_second_at=at,received_ms=start)
    await insert(connection,"price_samples",**sample,instrument_id=instrument,price=Decimal("100.123456789012345678"))
    await insert(connection,"polymarket_probability_samples",**sample,up_token_id="up",down_token_id="down")
    await insert(connection,"binance_futures_snapshots",**sample,symbol="BTCUSDT")
    await insert(connection,"binance_flow_1s",**sample,venue="binance",symbol="BTCUSDT")
    await insert(connection,"binance_book_1s",**sample,venue="binance",symbol="BTCUSDT",bid=Decimal("100"),ask=Decimal("101"),
                 bid_qty=Decimal("1"),ask_qty=Decimal("1"),mid=Decimal("100.5"),spread=Decimal("1"),spread_bps=Decimal("99.5"))
    await insert(connection,"binance_microstructure_1s",**sample,symbol="BTCUSDT",schema_version=1,
                 sample_jitter_ms=0,collector_healthy=True)
    await insert(connection,"binance_futures_oi_5m_summaries",symbol="BTCUSDT",source_window_start_ms=start-300_000,
                 source_window_end_ms=start,effective_market_id=market_id,binance_timestamp_ms=start,received_ms=start)
    await insert(connection,"polymarket_market_observations",record_id=uuid4(),market_id=market_id,kind="gamma_market",
                 received_wall_ns=start*1_000_000,received_monotonic_ns=sequence,status="ok",payload_hash=payload)
    await insert(connection,"polymarket_quote_observations",connection_id=session,market_id=market_id,
                 observed_wall_ns=start*1_000_000,observed_monotonic_ns=sequence,receive_sequence=sequence)
    await insert(connection,"polymarket_twap_events",connection_id=session,receive_sequence=sequence,instrument_id=instrument,
                 topic="crypto_prices_twap_sixty",symbol="btc/usd",window_s=60,provider_event_ms=start,
                 received_wall_ns=start*1_000_000,received_monotonic_ns=sequence,sample_second_ms=start,market_id=market_id,
                 price_e18=Decimal("100000000000000000000"),price=Decimal("100"))
    for table in RETIRED:
        await insert(connection,table,market_id=market_id)
    return market_id


@pytest.mark.skipif(not DSN, reason="requires a pre-provisioned disposable PostgreSQL database")
def test_postgres_child_first_cleanup_shared_evidence_boundaries_and_ghost_isolation():
    from price_collector.retention import expire_history, inspect_history

    async def scenario():
        connection = await asyncpg.connect(DSN, command_timeout=10)
        transaction = None
        try:
            identity = await connection.fetchrow("SELECT current_database() AS db,session_user AS role,inet_server_addr() AS address")
            assert identity["db"].startswith("ghost_checkpoint_b_validation_") and identity["db"] != "ghost_checkpoint_b_validation_"
            assert identity["role"] == "postgres" and identity["address"] is None
            transaction = connection.transaction()
            await transaction.start()
            now = await connection.fetchval("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
            now = (now//300_000-1)*300_000+12_345
            cutoff, floor = now-HISTORY_RETENTION_MS, retained_market_floor_ms(now)
            starts = (floor-600_000,floor-300_000,floor)
            assert starts[1] < cutoff < starts[2]

            # Optional retired tables are absent from the active schema. Their
            # minimal real FKs exercise cleanup ordering when they still exist.
            for table in RETIRED:
                parent = "polymarket_btc_5m_flip_evaluations" if table.endswith(("flip_events","flip_cutoffs")) else (
                    "polymarket_btc_5m_markets" if table.endswith("flip_evaluations") else "market_windows")
                await connection.execute(f"CREATE TABLE {table}(market_id bigint PRIMARY KEY REFERENCES {parent}(market_id))")
            # Two physical partitions deliberately reuse ctid=(0,1). Cleanup
            # must keep tableoid with ctid so the retained partition is safe.
            await connection.execute(f"CREATE TABLE raw_capture.retention_smoke_old PARTITION OF raw_capture.chainlink_price_events FOR VALUES FROM(0) TO({cutoff*1_000_000})")
            await connection.execute("CREATE TABLE raw_capture.retention_smoke_current PARTITION OF raw_capture.chainlink_price_events DEFAULT")
            await connection.execute("CREATE TABLE raw_capture.retention_smoke_trace PARTITION OF raw_capture.binance_futures_price_trace_100ms DEFAULT")
            instrument = await connection.fetchval("SELECT instrument_id FROM instruments WHERE symbol='BTCUSD_TWAP_60S'")
            assert instrument is not None, "apply the full production schema and seeds first"
            shared_session, old_session, raw_session = uuid4(),uuid4(),uuid4()
            for session, closed in ((shared_session,False),(old_session,True)):
                fields = dict(connection_id=session,topic="crypto_prices_twap_sixty",symbol="btc/usd",window_s=60,
                              connected_wall_ns=starts[0]*1_000_000,connected_monotonic_ns=1,
                              subscribed_wall_ns=starts[0]*1_000_000,subscribed_monotonic_ns=1)
                if closed:
                    fields.update(disconnected_wall_ns=(cutoff-1)*1_000_000,disconnected_monotonic_ns=2,close_reason="test")
                await insert(connection,"polymarket_twap_sessions",**fields)
                await insert(connection,"polymarket_twap_gaps",connection_id=session,detected_wall_ns=(cutoff-1)*1_000_000,
                             detected_monotonic_ns=2,reason="test",messages_received_total=0,messages_accepted_total=0,parse_errors_total=0)
            hashes = {}
            for name, created in (("shared",starts[0]),("old_only",starts[0]),("old_orphan",starts[0]),("fresh_orphan",now)):
                body = json.dumps({"retention_fixture":name})
                hashes[name] = sha256(body.encode()).hexdigest()
                await insert(connection,"polymarket_evidence_payloads",payload_hash=hashes[name],payload=body,
                             created_at=EPOCH+timedelta(milliseconds=created))
            market_ids = []
            for index,start in enumerate(starts):
                market_ids.append(await seed_market(connection,start,instrument,shared_session,index+1,hashes["shared"]))
            await insert(connection,"polymarket_market_observations",record_id=uuid4(),market_id=market_ids[0],kind="gamma_market",
                         received_wall_ns=starts[0]*1_000_000,received_monotonic_ns=4,status="ok",payload_hash=hashes["old_only"])
            # A late sample in the straddling market is intentionally expired
            # with its whole market, even though its timestamp is newer than cutoff.
            await insert(connection,"price_samples",instrument_id=instrument,sample_second_ms=floor-1000,
                         sample_second_at=EPOCH+timedelta(milliseconds=floor-1000),market_id=market_ids[1],
                         price=Decimal("100"),received_ms=floor-1000)
            await insert(connection,"raw_capture.feed_sessions",connection_id=raw_session,source="polymarket_chainlink_rtds",
                         connected_wall_ns=starts[0]*1_000_000,connected_monotonic_ns=1)
            for sequence,stamp in enumerate((cutoff-1,cutoff),1):
                await insert(connection,"raw_capture.chainlink_price_events",received_wall_ns=stamp*1_000_000,
                             received_monotonic_ns=sequence,connection_id=raw_session,receive_sequence=sequence,
                             provider_event_ms=stamp,price=Decimal("100"))
            bucket = (cutoff//100-1)*100
            await insert(connection,"raw_capture.binance_futures_price_trace_100ms",bucket_start_ms=bucket,connection_id=uuid4(),
                         first_received_wall_ns=bucket*1_000_000,last_received_wall_ns=bucket*1_000_000,
                         first_received_monotonic_ns=1,last_received_monotonic_ns=1,
                         first_trade_time_ms=bucket,last_trade_time_ms=bucket,first_event_time_ms=bucket,last_event_time_ms=bucket,
                         open_price=Decimal("100"),high_price=Decimal("100"),low_price=Decimal("100"),close_price=Decimal("100"),
                         event_count=1,first_agg_trade_id=1,last_agg_trade_id=1)
            ghost_id = str(uuid4())
            await insert(connection,"ghost_twap_audit",run_id=ghost_id,decision_id="untouched",decision_wall_ns=starts[0]*1_000_000,
                         created_ms=starts[0],frozen_json="{}",state_json="{}",version=1,terminal=True)
            ghost_before = await connection.fetchval("SELECT row_to_json(a)::text FROM ghost_twap_audit a WHERE run_id=$1",ghost_id)
            count_before = await connection.fetchval("SELECT count(*) FROM price_samples WHERE market_id=ANY($1::bigint[])",market_ids)
            inspected = await inspect_history(connection,now)
            assert inspected is not None
            assert await connection.fetchval("SELECT count(*) FROM price_samples WHERE market_id=ANY($1::bigint[])",market_ids) == count_before
            result = await expire_history(connection,now,max_seconds=15,batch_size=1)
            assert result["errors"] == {}, result
            assert result["cutoff_ms"] == cutoff and result["market_floor_ms"] == floor
            assert sum(result["deleted"].values()) > 20
            kept = await connection.fetch("SELECT market_id FROM market_windows WHERE market_id=ANY($1::bigint[]) ORDER BY market_id",market_ids)
            assert [r["market_id"] for r in kept] == [market_ids[-1]]
            for table in ("price_samples","polymarket_probability_samples","polymarket_market_observations",
                          "polymarket_quote_observations","polymarket_twap_events","polymarket_btc_5m_resolutions",
                          "binance_futures_snapshots","binance_flow_1s","binance_book_1s","binance_microstructure_1s",*RETIRED):
                assert await connection.fetchval(f"SELECT count(*) FROM {table} WHERE market_id=ANY($1::bigint[])",market_ids) == 1, table
            # OI labels the following window; even the first retained label has
            # an expired source window in this fixture.
            assert await connection.fetchval("SELECT count(*) FROM binance_futures_oi_5m_summaries WHERE effective_market_id=ANY($1::bigint[])",market_ids) == 0
            assert await connection.fetchval("SELECT count(*) FROM polymarket_twap_sessions WHERE connection_id=$1",shared_session) == 1
            assert await connection.fetchval("SELECT count(*) FROM polymarket_twap_sessions WHERE connection_id=$1",old_session) == 0
            surviving = set(await connection.fetchval("SELECT array_agg(payload_hash) FROM polymarket_evidence_payloads WHERE payload_hash=ANY($1::text[])",list(hashes.values())))
            assert surviving == {hashes["shared"],hashes["fresh_orphan"]}
            assert await connection.fetchval("SELECT array_agg(provider_event_ms) FROM raw_capture.chainlink_price_events WHERE connection_id=$1",raw_session) == [cutoff]
            assert await connection.fetchval("SELECT count(*) FROM raw_capture.feed_sessions WHERE connection_id=$1",raw_session) == 1
            assert await connection.fetchval("SELECT row_to_json(a)::text FROM ghost_twap_audit a WHERE run_id=$1",ghost_id) == ghost_before
            again = await expire_history(connection,now,max_seconds=5,batch_size=1)
            assert again["errors"] == {} and sum(again["deleted"].values()) == 0
        finally:
            if transaction is not None:
                await transaction.rollback()
            await connection.close()

    asyncio.run(asyncio.wait_for(scenario(),timeout=40))
