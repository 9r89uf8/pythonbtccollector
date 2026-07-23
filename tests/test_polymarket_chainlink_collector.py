import asyncio
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

import price_collector.polymarket_chainlink_collector as collector
import price_collector.raw_capture as raw_capture
from price_collector.live_cache import CHAINLINK_LIVE_KEY


def polymarket_settings():
    return SimpleNamespace(
        POLYMARKET_CHAINLINK_TOPIC="crypto_prices_chainlink",
        POLYMARKET_CHAINLINK_RTD_SYMBOL="btc/usd",
    )


def valid_message(**overrides):
    message = {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": 1_783_459_200_456,
        "payload": {
            "symbol": "btc/usd",
            "value": Decimal("123456.780000000000000000"),
            "timestamp": 1_783_459_200_123,
        },
    }
    message.update(overrides)
    return message


def startup_history_snapshot(**overrides):
    message = {
        "topic": "crypto_prices",
        "type": "subscribe",
        "timestamp": 1_783_459_200_400,
        "payload": {
            "symbol": "btc/usd",
            "data": [
                {
                    "timestamp": 1_783_459_199_000,
                    "value": "123455.50",
                }
            ],
        },
    }
    message.update(overrides)
    return message


def chainlink_sample(
    *,
    price="123456.78",
    provider_event_ms=1_783_459_200_123,
    provider_message_ms=1_783_459_200_456,
    received_ms=1_783_459_200_500,
):
    return collector.build_polymarket_chainlink_sample(
        collector.PolymarketChainlinkTick(
            symbol="BTCUSD",
            price=Decimal(price),
            provider_event_ms=provider_event_ms,
            provider_message_ms=provider_message_ms,
        ),
        received_ms=received_ms,
    )


def reader_settings(*, idle_timeout_ms=10_000):
    return SimpleNamespace(
        POLYMARKET_RTDS_WS_URL="wss://example.test/rtds",
        POLYMARKET_CHAINLINK_TOPIC="crypto_prices_chainlink",
        POLYMARKET_CHAINLINK_RTD_SYMBOL="btc/usd",
        POLYMARKET_CHAINLINK_SYMBOL="BTCUSD",
        POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=(
            idle_timeout_ms
        ),
    )


class ScriptedRtdsWebSocket:
    def __init__(self, messages, *, second_message_gate=None):
        self.messages = list(messages)
        self.second_message_gate = second_message_gate
        self.sent = []
        self._index = 0

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self.messages):
            await asyncio.Future()
        if self._index == 1 and self.second_message_gate is not None:
            await self.second_message_gate.wait()
        message = self.messages[self._index]
        self._index += 1
        return message


class ControlAndMalformedRtdsWebSocket:
    def __init__(self):
        self.sent = []
        self._index = 0

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.005)
        messages = ("PONG", "not-json")
        message = messages[self._index % len(messages)]
        self._index += 1
        return message


class StartupFramesOnlyRtdsWebSocket:
    def __init__(self):
        self.sent = []
        self._index = 0
        self._messages = (
            "",
            json.dumps(startup_history_snapshot()),
        )

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.005)
        message = self._messages[self._index % len(self._messages)]
        self._index += 1
        return message


class WebSocketContext:
    def __init__(self, websocket):
        self.websocket = websocket

    async def __aenter__(self):
        return self.websocket

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class RecordingRawCapture:
    def __init__(self, *, fail_events=False):
        self.counters = raw_capture.CaptureCounters()
        self.records = []
        self.fail_events = fail_events
        self.buffer = SimpleNamespace(qsize=lambda: len(self.records))

    def offer_nowait(self, record):
        if self.fail_events and isinstance(record, raw_capture.ChainlinkPriceEvent):
            raise RuntimeError("raw event sink failed")
        self.records.append(record)
        return raw_capture.OfferResult(
            accepted=True,
            dropped_oldest=False,
            dropped_record=None,
            queue_depth=len(self.records),
            queue_high_water=len(self.records),
        )


async def wait_for_delivery_sequence(state, expected):
    while state.telemetry_fields(now_ms=1)["delivery_sequence"] < expected:
        await asyncio.sleep(0)


def test_polymarket_subscription_uses_chainlink_topic_and_btc_usd_filter():
    subscription = collector.build_polymarket_chainlink_subscription(polymarket_settings())

    assert subscription["action"] == "subscribe"
    rtds_subscription = subscription["subscriptions"][0]
    assert rtds_subscription["topic"] == "crypto_prices_chainlink"
    assert rtds_subscription["topic"] != "crypto_prices"
    assert rtds_subscription["type"] == "*"
    assert rtds_subscription["filters"] == '{"symbol":"btc/usd"}'
    assert json.loads(rtds_subscription["filters"]) == {"symbol": "btc/usd"}
    assert "btcusdt" not in rtds_subscription["filters"]


def test_parse_polymarket_chainlink_message_uses_decimal_value_and_payload_timestamp():
    raw_message = """
    {
      "topic": "crypto_prices_chainlink",
      "type": "update",
      "timestamp": 1783459200456,
      "payload": {
        "symbol": "btc/usd",
        "value": 123456.780000000000000000,
        "timestamp": 1783459200123
      }
    }
    """
    message = json.loads(raw_message, parse_float=Decimal)

    tick = collector.parse_polymarket_chainlink_message(message)

    assert isinstance(message["payload"]["value"], Decimal)
    assert tick.symbol == "BTCUSD"
    assert tick.price == Decimal("123456.780000000000000000")
    assert tick.provider_event_ms == 1_783_459_200_123
    assert tick.provider_message_ms == 1_783_459_200_456


def test_parse_polymarket_chainlink_message_rejects_binance_sourced_rtds_topic():
    with pytest.raises(collector.RtdsParseError, match="unexpected RTDS topic"):
        collector.parse_polymarket_chainlink_message(valid_message(topic="crypto_prices"))


def test_parse_polymarket_chainlink_message_rejects_btcusdt_symbol():
    message = valid_message(
        payload={
            "symbol": "btcusdt",
            "value": Decimal("123456.78"),
            "timestamp": 1_783_459_200_123,
        }
    )

    with pytest.raises(collector.RtdsParseError, match="unexpected Chainlink symbol"):
        collector.parse_polymarket_chainlink_message(message)


@pytest.mark.parametrize(
    "message",
    [
        valid_message(
            payload={
                "symbol": "btc/usd",
                "value": raw_capture.POSTGRES_NUMERIC_38_18_LIMIT,
                "timestamp": 1_783_459_200_123,
            }
        ),
        valid_message(
            payload={
                "symbol": "btc/usd",
                "value": Decimal("123456.78"),
                "timestamp": raw_capture.POSTGRES_BIGINT_MAX + 1,
            }
        ),
        valid_message(
            payload={
                "symbol": "btc/usd",
                "value": Decimal("123456.78"),
                "timestamp": 1_000_000_000_000_000,
            }
        ),
    ],
    ids=[
        "numeric-overflow",
        "provider-event-bigint-overflow",
        "provider-event-datetime-overflow",
    ],
)
def test_parse_rejects_values_that_cannot_be_persisted(message):
    with pytest.raises(collector.RtdsParseError):
        collector.parse_polymarket_chainlink_message(message)


def test_parse_discards_optional_message_time_that_exceeds_postgres_bigint():
    tick = collector.parse_polymarket_chainlink_message(
        valid_message(timestamp=raw_capture.POSTGRES_BIGINT_MAX + 1)
    )

    assert tick.provider_message_ms is None


def test_sample_second_uses_payload_timestamp_floored_to_second():
    assert collector.sample_second_ms_for_provider_event(1_783_459_200_999) == 1_783_459_200_000


def test_exact_five_minute_boundary_belongs_to_new_market_from_payload_timestamp():
    tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123456.78"),
        provider_event_ms=1_783_459_500_000,
        provider_message_ms=1_783_459_500_010,
    )

    sample = collector.build_polymarket_chainlink_sample(
        tick,
        received_ms=1_783_459_500_020,
    )

    assert sample.sample_second_ms == 1_783_459_500_000
    assert sample.window.market_start_ms == 1_783_459_500_000
    assert sample.window.market_end_ms == 1_783_459_800_000


def test_delivery_state_collapses_same_second_to_latest_received_version():
    state = collector.ChainlinkDeliveryState(history_settle_ms=1_000)
    first = state.update_latest(
        chainlink_sample(
            price="62000.01",
            provider_event_ms=1_783_459_200_100,
            received_ms=1_783_459_200_200,
        )
    )
    state.offer_history(first)
    second = state.update_latest(
        chainlink_sample(
            price="62001.25",
            provider_event_ms=1_783_459_200_900,
            received_ms=1_783_459_201_100,
        )
    )
    state.offer_history(second)

    assert state.history_pending_count == 1
    assert state.history_collapsed_total == 1
    ready = state.next_history_ready(now_ms=1_783_459_202_100)
    assert ready == second
    assert ready.sample.price == Decimal("62001.25")
    assert ready.sample.provider_event_ms == 1_783_459_200_900
    assert ready.sample.received_ms == 1_783_459_201_100


def test_internal_delivery_sequence_is_process_stable_across_reconnects():
    state = collector.ChainlinkDeliveryState()

    first = state.update_latest(chainlink_sample())
    state.raw_connection_opened(UUID("11111111-1111-4111-8111-111111111111"))
    state.raw_connection_closed(UUID("11111111-1111-4111-8111-111111111111"))
    state.raw_connection_opened(UUID("22222222-2222-4222-8222-222222222222"))
    second = state.update_latest(chainlink_sample(price="62001.00"))

    assert (first.sequence, second.sequence) == (1, 2)


def test_delivery_state_keeps_newer_version_arriving_during_inflight_write():
    state = collector.ChainlinkDeliveryState(history_settle_ms=0)
    first = state.update_latest(chainlink_sample(price="62000.00"))
    state.offer_history(first)
    assert state.next_history_ready(now_ms=first.sample.received_ms) == first

    newer = state.update_latest(
        chainlink_sample(
            price="62010.00",
            provider_event_ms=1_783_459_200_999,
            received_ms=1_783_459_200_999,
        )
    )
    state.offer_history(newer)
    state.mark_history_succeeded(first, written_ms=1_783_459_201_000)

    assert state.history_pending_count == 1
    assert state.next_history_ready(now_ms=1_783_459_201_000) == newer


def test_live_attempt_barrier_keeps_redis_before_postgres(monkeypatch):
    async def scenario():
        state = collector.ChainlinkDeliveryState(history_settle_ms=0)
        redis_started = asyncio.Event()
        release_redis = asyncio.Event()
        postgres_written = asyncio.Event()
        events = []

        class BlockingLiveCache:
            async def set_price(self, key, **kwargs):
                events.append(("redis_started", key, kwargs))
                redis_started.set()
                await release_redis.wait()
                events.append(("redis_finished", key, kwargs))

        async def fake_write(pool, instrument_id, sample, *, source_topic):
            events.append(("postgres", pool, instrument_id, sample, source_topic))
            postgres_written.set()

        monkeypatch.setattr(collector, "write_chainlink_sample", fake_write)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_202_000,
        )

        live_task = asyncio.create_task(
            collector.chainlink_live_worker(
                delivery_state=state,
                live_cache=BlockingLiveCache(),
            )
        )
        history_task = asyncio.create_task(
            collector.chainlink_history_worker(
                delivery_state=state,
                pool="pool",
                instrument_id=42,
                source_topic="crypto_prices_chainlink",
            )
        )
        item = state.update_latest(chainlink_sample())
        state.offer_history(item)

        await asyncio.wait_for(redis_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not postgres_written.is_set()

        release_redis.set()
        await asyncio.wait_for(postgres_written.wait(), timeout=1)
        assert [entry[0] for entry in events] == [
            "redis_started",
            "redis_finished",
            "postgres",
        ]
        redis_fields = events[0][2]
        assert redis_fields == {
            "value": item.sample.price,
            "source_timestamp_ms": item.sample.provider_event_ms,
            "received_ms": item.sample.received_ms,
        }

        state.close()
        await asyncio.wait_for(
            asyncio.gather(live_task, history_task),
            timeout=1,
        )

    asyncio.run(scenario())


def test_duplicate_ticks_in_same_source_second_use_same_upsert_key(monkeypatch):
    calls = []

    async def fake_upsert_price_sample(pool, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(collector, "upsert_price_sample", fake_upsert_price_sample)

    first_tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123456.78"),
        provider_event_ms=1_783_459_200_123,
        provider_message_ms=1_783_459_200_456,
    )
    second_tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("123457.01"),
        provider_event_ms=1_783_459_200_999,
        provider_message_ms=1_783_459_201_050,
    )

    asyncio.run(
        collector.handle_tick(
            "pool",
            42,
            first_tick,
            received_ms=1_783_459_200_500,
        )
    )
    asyncio.run(
        collector.handle_tick(
            "pool",
            42,
            second_tick,
            received_ms=1_783_459_201_100,
        )
    )

    assert calls[0]["instrument_id"] == 42
    assert calls[0]["sample_second_ms"] == 1_783_459_200_000
    assert calls[1]["sample_second_ms"] == 1_783_459_200_000
    assert calls[0]["source_price_field"] == "payload.value"
    assert calls[0]["source_topic"] == "crypto_prices_chainlink"
    assert calls[1]["price"] == Decimal("123457.01")


def test_chainlink_tick_writes_redis_live_cache_before_postgres(monkeypatch):
    events = []

    class FakeLiveCache:
        async def set_price(
            self,
            key,
            *,
            value,
            source_timestamp_ms,
            received_ms,
        ):
            events.append(
                (
                    "redis",
                    {
                        "key": key,
                        "value": value,
                        "source_timestamp_ms": source_timestamp_ms,
                        "received_ms": received_ms,
                    },
                )
            )

    async def fake_upsert_price_sample(pool, **kwargs):
        events.append(("postgres", kwargs))

    monkeypatch.setattr(collector, "upsert_price_sample", fake_upsert_price_sample)

    tick = collector.PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=Decimal("62066.12"),
        provider_event_ms=1_783_459_250_123,
        provider_message_ms=1_783_459_250_140,
    )

    asyncio.run(
        collector.handle_tick(
            "pool",
            42,
            tick,
            received_ms=1_783_459_250_180,
            live_cache=FakeLiveCache(),
        )
    )

    assert [event_name for event_name, _payload in events] == ["redis", "postgres"]
    assert events[0][1] == {
        "key": CHAINLINK_LIVE_KEY,
        "value": Decimal("62066.12"),
        "source_timestamp_ms": 1_783_459_250_123,
        "received_ms": 1_783_459_250_180,
    }
    assert events[1][1]["sample_second_ms"] == 1_783_459_250_000


def test_history_failure_retries_while_reader_keeps_receiving(monkeypatch):
    async def scenario():
        allow_second_message = asyncio.Event()
        first_write_started = asyncio.Event()
        release_first_write = asyncio.Event()
        writes = []
        first_message = valid_message()
        second_message = valid_message(
            timestamp=1_783_459_201_456,
            payload={
                "symbol": "btc/usd",
                "value": "123460.01",
                "timestamp": 1_783_459_201_123,
            },
        )
        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(first_message, default=str),
                json.dumps(second_message, default=str),
            ],
            second_message_gate=allow_second_message,
        )
        state = collector.ChainlinkDeliveryState(history_settle_ms=0)

        class FakeLiveCache:
            async def set_price(self, key, **kwargs):
                return None

        async def flaky_write(pool, instrument_id, sample, *, source_topic):
            writes.append(sample)
            if len(writes) == 1:
                first_write_started.set()
                await release_first_write.wait()
                raise RuntimeError("database unavailable")

        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(collector, "write_chainlink_sample", flaky_write)
        monkeypatch.setattr(collector, "reconnect_delay_seconds", lambda _attempt: 0)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 9_000_000_000_000,
        )

        live_task = asyncio.create_task(
            collector.chainlink_live_worker(
                delivery_state=state,
                live_cache=FakeLiveCache(),
            )
        )
        history_task = asyncio.create_task(
            collector.chainlink_history_worker(
                delivery_state=state,
                pool="pool",
                instrument_id=42,
                source_topic="crypto_prices_chainlink",
            )
        )
        reader_task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
            )
        )

        await asyncio.wait_for(first_write_started.wait(), timeout=1)
        allow_second_message.set()
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        assert not release_first_write.is_set()
        assert state.history_pending_count == 2

        release_first_write.set()
        while state.history_persisted_total < 2:
            await asyncio.sleep(0)
        assert state.history_failures_total == 1
        assert [sample.price for sample in writes] == [
            Decimal("123456.780000000000000000"),
            Decimal("123456.780000000000000000"),
            Decimal("123460.01"),
        ]

        reader_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reader_task
        state.close()
        await asyncio.wait_for(
            asyncio.gather(live_task, history_task),
            timeout=1,
        )

    asyncio.run(scenario())


def test_history_activity_does_not_bypass_retry_backoff():
    async def scenario():
        state = collector.ChainlinkDeliveryState(history_settle_ms=0)
        retry_wait = asyncio.create_task(state.wait_for_history_retry(60.0))
        await asyncio.sleep(0)

        item = state.update_latest(chainlink_sample())
        state.offer_history(item)

        done, _pending = await asyncio.wait({retry_wait}, timeout=0.01)
        assert not done

        state.close()
        await asyncio.wait_for(retry_wait, timeout=1)

    asyncio.run(scenario())


def test_reader_captures_wall_and_monotonic_clocks_before_json_parse(monkeypatch):
    async def scenario():
        calls = []
        wall_ns = [1_783_459_200_000_000_000]
        monotonic_ns = [1_000_000_000]
        original_loads = json.loads

        def next_wall_ns():
            calls.append("wall")
            wall_ns[0] += 10_000_000
            return wall_ns[0]

        def next_monotonic_ns():
            calls.append("monotonic")
            monotonic_ns[0] += 10_000_000
            return monotonic_ns[0]

        def recording_loads(*args, **kwargs):
            calls.append("parse")
            return original_loads(*args, **kwargs)

        websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(collector.time, "time_ns", next_wall_ns)
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(collector.json, "loads", recording_loads)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        parse_index = calls.index("parse")
        assert calls[parse_index - 2 : parse_index] == ["wall", "monotonic"]
        event = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        )
        assert event.received_wall_ns // 1_000_000 == (
            state.telemetry_fields(now_ms=1)["chainlink_received_ms"]
        )

    asyncio.run(scenario())


def test_same_wall_nanosecond_raw_events_remain_distinct_and_ordered(monkeypatch):
    async def scenario():
        wall_ns = 1_783_459_200_500_000_000
        monotonic_ns = [1_000_000_000]

        def next_monotonic_ns():
            monotonic_ns[0] += 1
            return monotonic_ns[0]

        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(valid_message(), default=str),
                json.dumps(
                    valid_message(
                        timestamp=1_783_459_200_457,
                        payload={
                            "symbol": "btc/usd",
                            "value": "123456.79",
                            "timestamp": 1_783_459_200_124,
                        },
                    ),
                    default=str,
                ),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(collector.time, "time_ns", lambda: wall_ns)
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        events = [
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        ]
        assert len(events) == 2
        assert [event.received_wall_ns for event in events] == [wall_ns, wall_ns]
        assert [event.receive_sequence for event in events] == [1, 2]
        assert events[0].connection_id == events[1].connection_id
        assert [event.price for event in events] == [
            Decimal("123456.780000000000000000"),
            Decimal("123456.79"),
        ]

    asyncio.run(scenario())


def test_pong_and_malformed_messages_are_counted_without_raw_events(monkeypatch):
    async def scenario():
        monotonic_ns = [1_000_000_000]

        def next_monotonic_ns():
            monotonic_ns[0] += 1
            return monotonic_ns[0]

        websocket = ScriptedRtdsWebSocket(
            ["PONG", "not-json", json.dumps(valid_message(), default=str)]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(
            collector.time,
            "time_ns",
            lambda: 1_783_459_200_500_000_000,
        )
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        event = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        )
        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert event.receive_sequence == 3
        assert closed.messages_received_total == 3
        assert closed.messages_accepted_total == 1
        assert closed.parse_errors_total == 1
        assert raw.counters.messages_received_total == 3
        assert raw.counters.messages_accepted_total == 1
        assert raw.counters.parse_errors_total == 1

    asyncio.run(scenario())


def test_expected_startup_frames_are_received_without_parse_errors(monkeypatch):
    async def scenario():
        monotonic_ns = [1_000_000_000]

        def next_monotonic_ns():
            monotonic_ns[0] += 1
            return monotonic_ns[0]

        websocket = ScriptedRtdsWebSocket(
            [
                "",
                json.dumps(startup_history_snapshot()),
                json.dumps(valid_message(), default=str),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(
            collector.time,
            "time_ns",
            lambda: 1_783_459_200_500_000_000,
        )
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        event = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        )
        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert event.receive_sequence == 3
        assert closed.messages_received_total == 3
        assert closed.messages_accepted_total == 1
        assert closed.parse_errors_total == 0
        assert raw.counters.messages_received_total == 3
        assert raw.counters.messages_accepted_total == 1
        assert raw.counters.parse_errors_total == 0

    asyncio.run(scenario())


def test_binary_startup_and_malformed_frames_keep_connection_accounting(
    monkeypatch,
):
    async def scenario():
        monotonic_ns = [1_000_000_000]

        def next_monotonic_ns():
            monotonic_ns[0] += 1
            return monotonic_ns[0]

        websocket = ScriptedRtdsWebSocket(
            [
                b"",
                json.dumps(startup_history_snapshot()).encode("utf-8"),
                b"\xff",
                json.dumps(valid_message(), default=str).encode("utf-8"),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(
            collector.time,
            "time_ns",
            lambda: 1_783_459_200_500_000_000,
        )
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        event = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        )
        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert event.receive_sequence == 4
        assert closed.messages_received_total == 4
        assert closed.messages_accepted_total == 1
        assert closed.parse_errors_total == 1
        assert raw.counters.messages_received_total == 4
        assert raw.counters.messages_accepted_total == 1
        assert raw.counters.parse_errors_total == 1

    asyncio.run(scenario())


def test_startup_shaped_and_malformed_frames_after_first_tick_are_parse_errors(
    monkeypatch,
):
    async def scenario():
        monotonic_ns = [1_000_000_000]

        def next_monotonic_ns():
            monotonic_ns[0] += 1
            return monotonic_ns[0]

        second_tick = valid_message(
            timestamp=1_783_459_201_456,
            payload={
                "symbol": "btc/usd",
                "value": "123460.01",
                "timestamp": 1_783_459_201_123,
            },
        )
        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(valid_message(), default=str),
                "",
                json.dumps(startup_history_snapshot()),
                "not-json",
                json.dumps(second_tick),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(
            collector.time,
            "time_ns",
            lambda: 1_783_459_201_500_000_000,
        )
        monkeypatch.setattr(collector.time, "monotonic_ns", next_monotonic_ns)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        events = [
            record
            for record in raw.records
            if isinstance(record, raw_capture.ChainlinkPriceEvent)
        ]
        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert [event.receive_sequence for event in events] == [1, 5]
        assert closed.messages_received_total == 5
        assert closed.messages_accepted_total == 2
        assert closed.parse_errors_total == 3
        assert raw.counters.messages_received_total == 5
        assert raw.counters.messages_accepted_total == 2
        assert raw.counters.parse_errors_total == 3

    asyncio.run(scenario())


def test_startup_frames_do_not_reset_accepted_tick_idle_deadline(
    monkeypatch,
    caplog,
):
    async def scenario():
        stale_websocket = StartupFramesOnlyRtdsWebSocket()
        recovered_websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        websockets = [stale_websocket, recovered_websocket]
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        connect_calls = 0
        reconnect_attempts = []

        def connect(*args, **kwargs):
            nonlocal connect_calls
            websocket = websockets[min(connect_calls, len(websockets) - 1)]
            connect_calls += 1
            return WebSocketContext(websocket)

        monkeypatch.setattr(collector.websockets, "connect", connect)

        def reconnect_delay(attempt):
            reconnect_attempts.append(attempt)
            return 0

        monkeypatch.setattr(
            collector,
            "reconnect_delay_seconds",
            reconnect_delay,
        )
        caplog.set_level(
            "WARNING",
            logger="price_collector.polymarket_chainlink_collector",
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(idle_timeout_ms=100),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)

        assert connect_calls == 2
        assert reconnect_attempts == [1]
        timeout_record = next(
            record
            for record in caplog.records
            if getattr(record, "event", None)
            == "polymarket_rtds_idle_reconnect_triggered"
        )
        assert timeout_record.connection_messages_received_total > 0
        assert timeout_record.connection_messages_accepted_total == 0
        assert timeout_record.connection_parse_errors_total == 0
        assert timeout_record.frame_idle_ms < timeout_record.accepted_tick_idle_ms

        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert closed.close_reason == "proactive_reconnect"
        assert closed.messages_received_total > 0
        assert closed.messages_accepted_total == 0
        assert closed.parse_errors_total == 0

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_idle_reconnect_watchdog_ignores_non_price_frames_and_recovers(
    monkeypatch,
    caplog,
):
    async def scenario():
        stale_websockets = [
            ControlAndMalformedRtdsWebSocket(),
            ControlAndMalformedRtdsWebSocket(),
        ]
        recovered_websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        websockets = [*stale_websockets, recovered_websocket]
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        connect_calls = 0
        reconnect_attempts = []

        def connect(*args, **kwargs):
            nonlocal connect_calls
            websocket = websockets[min(connect_calls, len(websockets) - 1)]
            connect_calls += 1
            return WebSocketContext(websocket)

        monkeypatch.setattr(collector.websockets, "connect", connect)

        def reconnect_delay(attempt):
            reconnect_attempts.append(attempt)
            return 0

        monkeypatch.setattr(
            collector,
            "reconnect_delay_seconds",
            reconnect_delay,
        )
        caplog.set_level(
            "WARNING",
            logger="price_collector.polymarket_chainlink_collector",
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(idle_timeout_ms=100),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)

        assert connect_calls == 3
        assert reconnect_attempts == [1, 2]
        timeout_records = [
            record
            for record in caplog.records
            if getattr(record, "event", None)
            == "polymarket_rtds_idle_reconnect_triggered"
        ]
        assert len(timeout_records) == 2
        timeout_record = timeout_records[0]
        assert timeout_record.idle_basis == "accepted_chainlink_tick"
        assert timeout_record.idle_timeout_ms == 100
        assert timeout_record.accepted_tick_idle_ms > 0
        assert (
            timeout_record.frame_idle_ms
            < timeout_record.accepted_tick_idle_ms
        )
        assert timeout_record.connection_messages_received_total > 0
        assert timeout_record.connection_messages_accepted_total == 0
        assert timeout_record.connection_parse_errors_total > 0
        assert timeout_record.idle_reconnects_total == 1
        assert timeout_record.consecutive_idle_reconnects == 1
        assert [
            record.idle_reconnects_total for record in timeout_records
        ] == [1, 2]
        assert [
            record.consecutive_idle_reconnects
            for record in timeout_records
        ] == [1, 2]

        closed_sessions = [
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        ]
        assert [record.close_reason for record in closed_sessions] == [
            "proactive_reconnect",
            "proactive_reconnect",
        ]

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_idle_reconnect_watchdog_resets_only_after_accepted_ticks(
    monkeypatch,
    caplog,
):
    async def scenario():
        allow_second_message = asyncio.Event()
        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(valid_message(), default=str),
                json.dumps(
                    valid_message(
                        timestamp=1_783_459_201_456,
                        payload={
                            "symbol": "btc/usd",
                            "value": "123460.01",
                            "timestamp": 1_783_459_201_123,
                        },
                    ),
                    default=str,
                ),
            ],
            second_message_gate=allow_second_message,
        )
        state = collector.ChainlinkDeliveryState()
        connect_calls = 0

        def connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return WebSocketContext(websocket)

        monkeypatch.setattr(collector.websockets, "connect", connect)
        caplog.set_level(
            "WARNING",
            logger="price_collector.polymarket_chainlink_collector",
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(idle_timeout_ms=100),
                state,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        await asyncio.sleep(0.06)
        allow_second_message.set()
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        await asyncio.sleep(0.06)

        assert connect_calls == 1
        assert not any(
            getattr(record, "event", None)
            == "polymarket_rtds_idle_reconnect_triggered"
            for record in caplog.records
        )

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_raw_offer_failure_does_not_stop_chainlink_delivery(monkeypatch):
    async def scenario():
        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(valid_message(), default=str),
                json.dumps(
                    valid_message(
                        payload={
                            "symbol": "btc/usd",
                            "value": "123456.79",
                            "timestamp": 1_783_459_200_124,
                        }
                    ),
                    default=str,
                ),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture(fail_events=True)
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert state.history_pending_count == 1
        assert state.telemetry_fields(now_ms=1)["chainlink_latest_price"] == Decimal(
            "123456.79"
        )
        assert not any(
            isinstance(record, raw_capture.ChainlinkPriceEvent)
            for record in raw.records
        )
        closed = next(
            record
            for record in raw.records
            if isinstance(record, raw_capture.FeedSessionRecord)
            and record.close_reason is not None
        )
        assert closed.messages_accepted_total == 2
        assert closed.records_dropped_total == 2
        assert raw.counters.records_dropped_total == 2

    asyncio.run(scenario())


def test_malformed_raw_offer_result_does_not_reconnect_or_lose_normal_history(
    monkeypatch,
):
    async def scenario():
        websocket = ScriptedRtdsWebSocket(
            [
                json.dumps(valid_message(), default=str),
                json.dumps(
                    valid_message(
                        payload={
                            "symbol": "btc/usd",
                            "value": "123456.79",
                            "timestamp": 1_783_459_200_124,
                        }
                    ),
                    default=str,
                ),
            ]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        connect_calls = 0

        def malformed_event_result(record):
            if isinstance(record, raw_capture.ChainlinkPriceEvent):
                return None
            return RecordingRawCapture.offer_nowait(raw, record)

        def connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return WebSocketContext(websocket)

        monkeypatch.setattr(raw, "offer_nowait", malformed_event_result)
        monkeypatch.setattr(collector.websockets, "connect", connect)
        monkeypatch.setattr(collector, "reconnect_delay_seconds", lambda _attempt: 0)

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 2), timeout=1)
        await asyncio.sleep(0)

        assert connect_calls == 1
        assert state.history_pending_count == 1
        assert state.telemetry_fields(now_ms=1)["chainlink_latest_price"] == Decimal(
            "123456.79"
        )

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


@pytest.mark.parametrize("raw_failure", ["uuid", "monotonic"])
def test_raw_setup_and_clock_failures_do_not_interrupt_normal_delivery(
    monkeypatch,
    raw_failure,
):
    async def scenario():
        websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        state = collector.ChainlinkDeliveryState()
        raw = RecordingRawCapture()
        connect_calls = 0

        def connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return WebSocketContext(websocket)

        def fail_raw_operation():
            raise RuntimeError(f"raw {raw_failure} failed")

        monkeypatch.setattr(collector.websockets, "connect", connect)
        monkeypatch.setattr(collector, "reconnect_delay_seconds", lambda _attempt: 0)
        if raw_failure == "uuid":
            monkeypatch.setattr(collector, "uuid4", fail_raw_operation)
        else:
            monkeypatch.setattr(collector.time, "monotonic_ns", fail_raw_operation)

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)

        assert connect_calls == 1
        assert state.history_pending_count == 1
        assert state.telemetry_fields(now_ms=1)["chainlink_latest_price"] == Decimal(
            "123456.780000000000000000"
        )

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_raw_monitor_close_failure_does_not_mask_reader_cancellation(monkeypatch):
    async def scenario():
        class CloseFailingDeliveryState(collector.ChainlinkDeliveryState):
            def raw_connection_closed(self, connection_id):
                raise RuntimeError("raw connection monitor close failed")

        websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        state = CloseFailingDeliveryState()
        raw = RecordingRawCapture()
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
                raw_capture=raw,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        assert state.history_pending_count == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_raw_disabled_reader_avoids_uuid_and_monotonic_capture_work(monkeypatch):
    async def scenario():
        websocket = ScriptedRtdsWebSocket(
            [json.dumps(valid_message(), default=str)]
        )
        state = collector.ChainlinkDeliveryState()
        monkeypatch.setattr(
            collector,
            "uuid4",
            lambda: pytest.fail("disabled reader generated a connection UUID"),
        )
        monkeypatch.setattr(
            collector.time,
            "monotonic_ns",
            lambda: pytest.fail("disabled reader captured monotonic time"),
        )
        monkeypatch.setattr(
            collector.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            collector.polymarket_chainlink_reader_loop(
                reader_settings(),
                state,
            )
        )
        await asyncio.wait_for(wait_for_delivery_sequence(state, 1), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        fields = state.telemetry_fields(now_ms=1)
        assert fields["delivery_sequence"] == 1
        assert fields["chainlink_latest_connection_id"] is None

    asyncio.run(scenario())


def chainlink_run_settings(*, raw_enabled):
    return SimpleNamespace(
        LOG_LEVEL="INFO",
        APP_ENV="test",
        DATABASE_URL="postgresql://writer@localhost/price_collector",
        POLYMARKET_RTDS_WS_URL="wss://example.test/rtds",
        POLYMARKET_CHAINLINK_PROVIDER_CODE="polymarket_chainlink_rtds",
        POLYMARKET_CHAINLINK_SYMBOL="BTCUSD",
        POLYMARKET_CHAINLINK_RTD_SYMBOL="btc/usd",
        POLYMARKET_CHAINLINK_TOPIC="crypto_prices_chainlink",
        POLYMARKET_CHAINLINK_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=10_000,
        RAW_CHAINLINK_EVENTS_ENABLED=raw_enabled,
        RAW_FUTURES_TRACE_ENABLED=True,
        RAW_CAPTURE_RETENTION_HOURS=72,
        RAW_CAPTURE_MAX_RELATION_MB=2048,
        RAW_CAPTURE_QUEUE_MAX_EVENTS=5000,
        RAW_CAPTURE_BATCH_MAX_ROWS=500,
        RAW_CAPTURE_FLUSH_MS=1000,
        RAW_CAPTURE_RETENTION_CHECK_SECONDS=60,
        RAW_FUTURES_BUCKET_MS=100,
    )


def test_chainlink_sigterm_handler_cancels_current_task_and_is_removable(
    monkeypatch,
):
    calls = []

    class FakeTask:
        def cancel(self):
            calls.append("cancel")

    class FakeLoop:
        def add_signal_handler(self, sig, callback):
            calls.append(("add", sig))
            self.callback = callback

        def remove_signal_handler(self, sig):
            calls.append(("remove", sig))
            return True

    loop = FakeLoop()
    monkeypatch.setattr(collector.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(collector.asyncio, "current_task", lambda: FakeTask())

    remove_handler = collector._install_sigterm_cancellation()
    assert remove_handler is not None
    loop.callback()
    remove_handler()

    assert calls == [
        ("add", collector.signal.SIGTERM),
        "cancel",
        ("remove", collector.signal.SIGTERM),
    ]


def test_main_treats_collector_cancellation_as_clean_shutdown(monkeypatch):
    settings = object()

    def cancelled_run(coroutine):
        coroutine.close()
        raise asyncio.CancelledError

    monkeypatch.setattr(collector, "Settings", lambda: settings)
    monkeypatch.setattr(collector.asyncio, "run", cancelled_run)

    collector.main()


def test_run_collector_disabled_uses_no_raw_resources_even_if_futures_flag_is_true(
    monkeypatch,
):
    async def scenario():
        reader_started = asyncio.Event()
        events = []

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        async def fake_reader(settings, delivery_state, *, raw_capture=None):
            assert raw_capture is None
            reader_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("reader_stopped")

        async def fake_create_pool(database_url):
            assert database_url == "postgresql://writer@localhost/price_collector"
            return FakePool()

        async def fake_get_instrument_id(pool, *, provider_code, symbol):
            assert provider_code == "polymarket_chainlink_rtds"
            assert symbol == "BTCUSD"
            return 42

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(
            collector,
            "require_collector_database_url",
            lambda _settings: "postgresql://writer@localhost/price_collector",
        )
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "get_instrument_id", fake_get_instrument_id)
        monkeypatch.setattr(
            collector,
            "create_live_cache",
            lambda _settings: FakeLiveCache(),
        )
        monkeypatch.setattr(
            collector,
            "create_raw_capture_backend",
            lambda *args, **kwargs: pytest.fail(
                "disabled Chainlink capture created a raw backend"
            ),
        )
        monkeypatch.setattr(
            collector,
            "create_raw_capture_runtime",
            lambda **kwargs: pytest.fail(
                "disabled Chainlink capture created a raw runtime"
            ),
        )
        monkeypatch.setattr(
            collector,
            "polymarket_chainlink_reader_loop",
            fake_reader,
        )
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)

        task = asyncio.create_task(
            collector.run_collector(chainlink_run_settings(raw_enabled=False))
        )
        await asyncio.wait_for(reader_started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events == ["reader_stopped", "live_close", "pool_close"]

    asyncio.run(scenario())


def test_run_collector_bounds_reader_cancellation_before_closing_resources(
    monkeypatch,
):
    async def scenario():
        reader_started = asyncio.Event()
        reader_cancel_seen = asyncio.Event()
        release_reader = asyncio.Event()
        pool_closed = asyncio.Event()

        class FakePool:
            async def close(self):
                pool_closed.set()

        class FakeLiveCache:
            async def close(self):
                return None

        async def fake_create_pool(database_url):
            return FakePool()

        async def fake_get_instrument_id(pool, *, provider_code, symbol):
            return 42

        async def cancellation_resistant_reader(
            settings,
            delivery_state,
            *,
            raw_capture=None,
        ):
            reader_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                reader_cancel_seen.set()
                await release_reader.wait()
                raise

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(
            collector,
            "require_collector_database_url",
            lambda _settings: "postgresql://writer@localhost/price_collector",
        )
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "get_instrument_id", fake_get_instrument_id)
        monkeypatch.setattr(
            collector,
            "create_live_cache",
            lambda _settings: FakeLiveCache(),
        )
        monkeypatch.setattr(
            collector,
            "polymarket_chainlink_reader_loop",
            cancellation_resistant_reader,
        )
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        monkeypatch.setattr(
            collector,
            "CHAINLINK_DELIVERY_SHUTDOWN_TIMEOUT_SECONDS",
            0.01,
        )
        if hasattr(collector, "CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS"):
            monkeypatch.setattr(
                collector,
                "CHAINLINK_READER_SHUTDOWN_TIMEOUT_SECONDS",
                0.01,
            )

        task = asyncio.create_task(
            collector.run_collector(chainlink_run_settings(raw_enabled=False))
        )
        await asyncio.wait_for(reader_started.wait(), timeout=1)
        task.cancel()
        await asyncio.wait_for(reader_cancel_seen.wait(), timeout=1)

        cleanup_completed_before_release = True
        try:
            await asyncio.wait_for(pool_closed.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            cleanup_completed_before_release = False
        finally:
            release_reader.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleanup_completed_before_release

    asyncio.run(scenario())


def test_run_collector_bounds_delivery_worker_cancellation_before_cleanup(
    monkeypatch,
):
    async def scenario():
        live_worker_started = asyncio.Event()
        live_cancel_seen = asyncio.Event()
        release_live_worker = asyncio.Event()
        pool_closed = asyncio.Event()

        class FakePool:
            async def close(self):
                pool_closed.set()

        class FakeLiveCache:
            async def close(self):
                return None

        async def fake_create_pool(database_url):
            return FakePool()

        async def fake_get_instrument_id(pool, *, provider_code, symbol):
            return 42

        async def blocking_reader(settings, delivery_state, *, raw_capture=None):
            await asyncio.Event().wait()

        async def cancellation_resistant_live_worker(*, delivery_state, live_cache):
            live_worker_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                live_cancel_seen.set()
                await release_live_worker.wait()
                raise

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(
            collector,
            "require_collector_database_url",
            lambda _settings: "postgresql://writer@localhost/price_collector",
        )
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "get_instrument_id", fake_get_instrument_id)
        monkeypatch.setattr(
            collector,
            "create_live_cache",
            lambda _settings: FakeLiveCache(),
        )
        monkeypatch.setattr(
            collector,
            "polymarket_chainlink_reader_loop",
            blocking_reader,
        )
        monkeypatch.setattr(
            collector,
            "chainlink_live_worker",
            cancellation_resistant_live_worker,
        )
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)
        monkeypatch.setattr(
            collector,
            "CHAINLINK_DELIVERY_SHUTDOWN_TIMEOUT_SECONDS",
            0.01,
        )
        monkeypatch.setattr(
            collector,
            "CHAINLINK_CANCEL_CONFIRM_TIMEOUT_SECONDS",
            0.01,
        )

        task = asyncio.create_task(
            collector.run_collector(chainlink_run_settings(raw_enabled=False))
        )
        await asyncio.wait_for(live_worker_started.wait(), timeout=1)
        task.cancel()
        await asyncio.wait_for(live_cancel_seen.wait(), timeout=1)

        cleanup_completed_before_release = True
        try:
            await asyncio.wait_for(pool_closed.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            cleanup_completed_before_release = False
        finally:
            release_live_worker.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleanup_completed_before_release

    asyncio.run(scenario())


def test_run_collector_wires_chainlink_only_raw_runtime_and_closes_in_order(
    monkeypatch,
):
    async def scenario():
        events = []
        reader_started = asyncio.Event()
        telemetry_started = asyncio.Event()
        runtime_kwargs = {}
        reader_capture = []
        backend_calls = []

        class FakePool:
            async def close(self):
                events.append("pool_close")

        class FakeLiveCache:
            async def close(self):
                events.append("live_close")

        class FakeRawRuntime:
            def start(self):
                events.append("raw_start")

            async def close(self):
                events.append("raw_close")

        raw_runtime = FakeRawRuntime()

        async def fake_create_pool(database_url):
            return FakePool()

        async def fake_get_instrument_id(pool, *, provider_code, symbol):
            return 42

        async def fake_create_raw_capture_backend(database_url, **kwargs):
            backend_calls.append((database_url, kwargs))
            return "backend"

        def fake_create_raw_capture_runtime(**kwargs):
            runtime_kwargs.update(kwargs)
            return raw_runtime

        async def fake_reader(settings, delivery_state, *, raw_capture=None):
            reader_capture.append(raw_capture)
            reader_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("reader_stopped")

        async def fake_telemetry(*, raw_capture, delivery_state):
            assert raw_capture is raw_runtime
            telemetry_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("telemetry_stopped")

        monkeypatch.setattr(collector, "setup_logging", lambda _level: None)
        monkeypatch.setattr(
            collector,
            "require_collector_database_url",
            lambda _settings: "postgresql://writer@localhost/price_collector",
        )
        monkeypatch.setattr(collector, "create_pool", fake_create_pool)
        monkeypatch.setattr(collector, "get_instrument_id", fake_get_instrument_id)
        monkeypatch.setattr(
            collector,
            "create_live_cache",
            lambda _settings: FakeLiveCache(),
        )
        monkeypatch.setattr(
            collector,
            "create_raw_capture_backend",
            fake_create_raw_capture_backend,
        )
        monkeypatch.setattr(
            collector,
            "create_raw_capture_runtime",
            fake_create_raw_capture_runtime,
        )
        monkeypatch.setattr(
            collector,
            "polymarket_chainlink_reader_loop",
            fake_reader,
        )
        monkeypatch.setattr(
            collector,
            "_run_chainlink_telemetry_noncritical",
            fake_telemetry,
        )
        monkeypatch.setattr(collector, "_install_sigterm_cancellation", lambda: None)

        task = asyncio.create_task(
            collector.run_collector(chainlink_run_settings(raw_enabled=True))
        )
        await asyncio.wait_for(reader_started.wait(), timeout=1)
        await asyncio.wait_for(telemetry_started.wait(), timeout=1)

        assert backend_calls == []
        assert await runtime_kwargs["backend_factory"]() == "backend"
        assert backend_calls == [
            (
                "postgresql://writer@localhost/price_collector",
                {"retention_hours": 72, "max_relation_mb": 2048},
            )
        ]
        assert runtime_kwargs["futures_enabled"] is False
        assert runtime_kwargs["chainlink_enabled"] is True
        assert runtime_kwargs["queue_max_events"] == 5000
        assert runtime_kwargs["batch_max_rows"] == 500
        assert runtime_kwargs["flush_ms"] == 1000
        assert runtime_kwargs["maintenance_interval_seconds"] == 60
        assert runtime_kwargs["bucket_ms"] == 100
        assert reader_capture == [raw_runtime]

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert events.count("raw_start") == 1
        assert events.index("reader_stopped") < events.index("raw_close")
        assert events.index("telemetry_stopped") < events.index("raw_close")
        assert events.index("raw_close") < events.index("live_close")
        assert events.index("live_close") < events.index("pool_close")

    asyncio.run(scenario())


def test_chainlink_telemetry_summary_keeps_signed_provider_and_receive_lags(
    monkeypatch,
    caplog,
):
    async def scenario():
        state = collector.ChainlinkDeliveryState()
        item = state.update_latest(
            chainlink_sample(
                price="62000.50",
                provider_event_ms=1_783_459_200_200,
                provider_message_ms=1_783_459_200_150,
                received_ms=1_783_459_200_100,
            )
        )
        state.offer_history(item)
        connection_id = UUID("33333333-3333-3333-3333-333333333333")
        state.raw_connection_opened(connection_id)
        state.observe_raw_event(
            raw_capture.ChainlinkPriceEvent(
                received_wall_ns=1_783_459_200_100_000_000,
                received_monotonic_ns=1_000_000_000,
                connection_id=connection_id,
                receive_sequence=7,
                provider_event_ms=1_783_459_200_200,
                provider_message_ms=1_783_459_200_150,
                price=Decimal("62000.50"),
            )
        )

        fields = state.telemetry_fields(now_ms=1_783_459_200_300)
        assert fields["chainlink_provider_event_to_receive_ms"] == -100
        assert fields["chainlink_provider_message_to_receive_ms"] == -50
        assert fields["chainlink_provider_message_minus_event_ms"] == -50
        assert fields["chainlink_provider_event_age_ms"] == 100
        assert fields["chainlink_received_age_ms"] == 200
        assert fields["chainlink_latest_receive_sequence"] == 7
        assert fields["chainlink_latest_connection_id"] == connection_id
        for field_name in (
            "delivery_live_attempted_sequence",
            "delivery_live_attempts_total",
            "delivery_live_failures_total",
            "delivery_history_collapsed_total",
            "delivery_history_failures_total",
            "delivery_history_pending_dropped_total",
            "delivery_history_pending_seconds",
            "delivery_history_pending_high_water",
            "chainlink_raw_interarrival_ns",
            "chainlink_raw_max_interarrival_ns",
        ):
            assert field_name in fields

        raw = RecordingRawCapture()
        sleep_calls = 0

        async def one_interval_then_cancel(_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls > 1:
                raise asyncio.CancelledError

        monkeypatch.setattr(collector.asyncio, "sleep", one_interval_then_cancel)
        monkeypatch.setattr(
            collector,
            "current_utc_epoch_ms",
            lambda: 1_783_459_200_300,
        )
        caplog.set_level(
            "INFO",
            logger="price_collector.polymarket_chainlink_collector",
        )

        with pytest.raises(asyncio.CancelledError):
            await collector.chainlink_raw_capture_telemetry_loop(
                raw_capture=raw,
                delivery_state=state,
                interval_seconds=60,
            )

        summary = next(
            record
            for record in caplog.records
            if getattr(record, "event", None) == "raw_capture_summary"
        )
        assert summary.source == "polymarket_chainlink_rtds"
        assert summary.connection_id == connection_id
        assert summary.chainlink_provider_event_to_receive_ms == -100
        assert summary.chainlink_provider_message_to_receive_ms == -50
        assert summary.chainlink_provider_event_age_ms == 100
        assert summary.chainlink_received_age_ms == 200

    asyncio.run(scenario())


def test_delivery_state_bounds_pending_history_and_counts_oldest_drop(caplog):
    async def scenario():
        state = collector.ChainlinkDeliveryState(
            history_capacity_seconds=2,
            history_settle_ms=0,
        )
        caplog.set_level(
            "ERROR",
            logger="price_collector.polymarket_chainlink_collector",
        )
        items = []
        for offset, price in enumerate(("62000", "62001", "62002")):
            item = state.update_latest(
                chainlink_sample(
                    price=price,
                    provider_event_ms=1_783_459_200_100 + (offset * 1_000),
                    provider_message_ms=1_783_459_200_150 + (offset * 1_000),
                    received_ms=1_783_459_200_200 + (offset * 1_000),
                )
            )
            state.offer_history(item)
            items.append(item)

        assert state.history_pending_count == 2
        assert state.history_pending_dropped_total == 1
        assert state.history_pending_high_water == 2
        first_ready = state.next_history_ready(now_ms=1_783_459_210_000)
        assert first_ready == items[1]
        warning = next(
            record
            for record in caplog.records
            if getattr(record, "event", None)
            == "polymarket_chainlink_history_pending_dropped"
        )
        assert warning.sample_second_ms == items[0].sample.sample_second_ms
        assert warning.history_capacity_seconds == 2

    asyncio.run(scenario())


def test_delivery_state_capacity_one_stays_bounded_while_only_item_is_inflight():
    async def scenario():
        state = collector.ChainlinkDeliveryState(
            history_capacity_seconds=1,
            history_settle_ms=0,
        )
        first = state.update_latest(
            chainlink_sample(
                price="62000",
                provider_event_ms=1_783_459_200_100,
                received_ms=1_783_459_200_200,
            )
        )
        state.offer_history(first)
        assert state.next_history_ready(now_ms=1_783_459_201_000) == first

        second = state.update_latest(
            chainlink_sample(
                price="62001",
                provider_event_ms=1_783_459_201_100,
                received_ms=1_783_459_201_200,
            )
        )
        state.offer_history(second)

        assert state.history_pending_count <= 1
        assert state.history_pending_high_water <= 1
        assert state.history_pending_dropped_total == 1

    asyncio.run(scenario())


def test_rtds_ping_loop_sends_text_ping_every_five_seconds(monkeypatch):
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1:
            raise asyncio.CancelledError

    class FakeWebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, message):
            self.sent.append(message)

    fake_websocket = FakeWebSocket()
    monkeypatch.setattr(collector.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(collector.rtds_ping_loop(fake_websocket))

    assert sleeps == [5.0, 5.0]
    assert fake_websocket.sent == ["PING"]


def test_collector_module_does_not_connect_to_direct_chainlink_websocket():
    source = Path(collector.__file__).read_text()

    assert "wss://ws.dataengine.chain.link" not in source
    assert "ws.dataengine.chain.link" not in source
