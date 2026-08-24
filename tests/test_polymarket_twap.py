import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

import price_collector.polymarket_twap as twap


def twap_settings(*, idle_timeout_ms=10_000, queue_max_events=100):
    return SimpleNamespace(
        POLYMARKET_RTDS_WS_URL="wss://example.test/rtds",
        POLYMARKET_TWAP_PROVIDER_CODE="polymarket_chainlink_twap_rtds",
        POLYMARKET_TWAP_SYMBOL="BTCUSD_TWAP_60S",
        POLYMARKET_TWAP_RTD_SYMBOL="btc/usd",
        POLYMARKET_TWAP_TOPIC="crypto_prices_twap_sixty",
        POLYMARKET_TWAP_WINDOW_SECONDS=60,
        POLYMARKET_TWAP_ACCEPTED_EVENT_IDLE_TIMEOUT_MS=idle_timeout_ms,
        POLYMARKET_TWAP_PERSIST_QUEUE_MAX_EVENTS=queue_max_events,
        POLYMARKET_TWAP_PERSIST_SHUTDOWN_TIMEOUT_SECONDS=0.1,
    )


def valid_twap_message(**payload_overrides):
    payload = {
        "symbol": "btc/usd",
        "value": Decimal("1.25"),
        "full_accuracy_value": "64255113422936400000000",
        "timestamp": 1_786_665_600_123,
        "window_s": 60,
    }
    payload.update(payload_overrides)
    return {
        "topic": "crypto_prices_twap_sixty",
        "type": "update",
        "timestamp": 1_786_665_600_456,
        "payload": payload,
    }


def current_twap_message(**payload_overrides):
    now_ms = twap.time.time_ns() // 1_000_000
    payload_overrides.setdefault("timestamp", now_ms - 1_000)
    message = valid_twap_message(**payload_overrides)
    message["timestamp"] = now_ms
    return message


class AcknowledgingQueue(asyncio.Queue):
    """Test queue that models a committed SessionStart barrier."""

    async def put(self, item):
        await super().put(item)
        if (
            isinstance(item, twap.PolymarketTwapSessionStart)
            and item.persisted is not None
        ):
            item.persisted.set()


def twap_event(*, receive_sequence=1):
    tick = twap.parse_polymarket_twap_message(valid_twap_message())
    return twap.build_polymarket_twap_event(
        tick,
        instrument_id=77,
        connection_id=UUID("11111111-1111-1111-1111-111111111111"),
        receive_sequence=receive_sequence,
        topic="crypto_prices_twap_sixty",
        received_wall_ns=1_786_665_600_500_123_456,
        received_monotonic_ns=9_000_000_000,
    )


def test_twap_subscription_uses_raw_update_topic_and_compact_symbol_filter():
    subscription = twap.build_polymarket_twap_subscription(twap_settings())

    assert subscription == {
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_twap_sixty",
                "type": "update",
                "filters": '{"symbol":"btc/usd"}',
            }
        ],
    }


def test_twap_parser_uses_exact_e18_instead_of_rounded_display_value():
    tick = twap.parse_polymarket_twap_message(valid_twap_message())

    assert tick.symbol == "btc/usd"
    assert tick.window_s == 60
    assert tick.price_e18 == 64_255_113_422_936_400_000_000
    assert tick.price == Decimal("64255.113422936400000000")
    assert tick.provider_event_ms == 1_786_665_600_123
    assert tick.provider_message_ms == 1_786_665_600_456


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    (
        ("POLYMARKET_TWAP_PROVIDER_CODE", "wrong_provider"),
        ("POLYMARKET_TWAP_SYMBOL", "BTCUSD_TWAP_30S"),
        ("POLYMARKET_TWAP_RTD_SYMBOL", "eth/usd"),
        ("POLYMARKET_TWAP_TOPIC", "crypto_prices_twap_thirty"),
        ("POLYMARKET_TWAP_WINDOW_SECONDS", 30),
    ),
)
def test_runtime_rejects_noncanonical_twap_identity(field_name, bad_value):
    settings = twap_settings()
    setattr(settings, field_name, bad_value)

    with pytest.raises(ValueError, match=field_name):
        twap.validate_twap_runtime_identity(settings)


def test_twap_timestamp_plausibility_accepts_normal_lag_and_rejects_bad_units():
    tick = twap.parse_polymarket_twap_message(valid_twap_message())
    twap.validate_twap_timestamp_plausibility(
        tick,
        received_ms=tick.provider_event_ms + 2_800,
        accepted_event_idle_timeout_ms=10_000,
    )

    with pytest.raises(twap.RtdsTwapParseError, match="is stale"):
        twap.validate_twap_timestamp_plausibility(
            tick,
            received_ms=tick.provider_event_ms + 10_001,
            accepted_event_idle_timeout_ms=10_000,
        )
    with pytest.raises(twap.RtdsTwapParseError, match="future-dated"):
        twap.validate_twap_timestamp_plausibility(
            tick,
            received_ms=tick.provider_event_ms - 2_001,
            accepted_event_idle_timeout_ms=10_000,
        )

    seconds_unit_tick = twap.parse_polymarket_twap_message(
        valid_twap_message(timestamp=1_786_665_600)
    )
    with pytest.raises(twap.RtdsTwapParseError, match="is stale"):
        twap.validate_twap_timestamp_plausibility(
            seconds_unit_tick,
            received_ms=1_786_665_600_500,
            accepted_event_idle_timeout_ms=10_000,
        )


@pytest.mark.parametrize(
    ("payload_overrides", "match"),
    (
        ({"window_s": 30}, "unexpected TWAP window_s"),
        ({"window_s": True}, "payload.window_s must be a positive integer"),
        ({"symbol": "eth/usd"}, "unexpected TWAP symbol"),
        (
            {"full_accuracy_value": "64255.1"},
            "payload.full_accuracy_value must be a positive integer",
        ),
        (
            {"full_accuracy_value": "-1"},
            "payload.full_accuracy_value must be a positive integer",
        ),
        (
            {"full_accuracy_value": None},
            "missing payload.full_accuracy_value",
        ),
    ),
)
def test_twap_parser_rejects_wrong_feed_or_inexact_e18(
    payload_overrides,
    match,
):
    with pytest.raises(twap.RtdsTwapParseError, match=match):
        twap.parse_polymarket_twap_message(
            valid_twap_message(**payload_overrides)
        )


def test_twap_parser_rejects_legacy_thirty_second_topic():
    message = valid_twap_message()
    message["topic"] = "crypto_prices_twap_thirty"

    with pytest.raises(twap.RtdsTwapParseError, match="unexpected TWAP topic"):
        twap.parse_polymarket_twap_message(message)


def test_twap_event_materialization_uses_provider_second_and_half_open_market():
    event = twap_event()

    assert event.sample_second_ms == 1_786_665_600_000
    assert event.window.market_start_ms == 1_786_665_600_000
    assert event.window.market_end_ms == 1_786_665_900_000
    assert event.window.market_id == 1_786_665_600_000 // 300_000
    assert event.price_e18 == 64_255_113_422_936_400_000_000
    assert isinstance(event.price, Decimal)


class WebSocketContext:
    def __init__(self, websocket):
        self.websocket = websocket

    async def __aenter__(self):
        return self.websocket

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class OneMessageWebSocket:
    def __init__(self, message, stage):
        self.message = message
        self.stage = stage
        self.sent = []
        self.returned = False

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.returned:
            await asyncio.Future()
        self.returned = True
        self.stage.append("recv_return")
        return self.message


def test_reader_stamps_wall_and_monotonic_immediately_before_parsing(monkeypatch):
    async def scenario():
        stage = []
        websocket = OneMessageWebSocket(
            json.dumps(current_twap_message(), default=str),
            stage,
        )
        records = AcknowledgingQueue(maxsize=103)
        original_time_ns = twap.time.time_ns
        original_monotonic_ns = twap.time.monotonic_ns
        original_parse = twap.parse_polymarket_twap_message

        def stamped_wall_ns():
            stage.append("wall")
            return original_time_ns()

        def stamped_monotonic_ns():
            stage.append("monotonic")
            return original_monotonic_ns()

        def recording_parse(*args, **kwargs):
            stage.append("parse")
            return original_parse(*args, **kwargs)

        monkeypatch.setattr(twap.time, "time_ns", stamped_wall_ns)
        monkeypatch.setattr(twap.time, "monotonic_ns", stamped_monotonic_ns)
        monkeypatch.setattr(twap, "parse_polymarket_twap_message", recording_parse)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )

        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
            )
        )
        while not any(isinstance(row, twap.PolymarketTwapEvent) for row in records._queue):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        recv_index = stage.index("recv_return")
        parse_index = stage.index("parse", recv_index)
        between = stage[recv_index + 1 : parse_index]
        assert between[:2] == ["wall", "monotonic"]

        persisted_event = next(
            row
            for row in records._queue
            if isinstance(row, twap.PolymarketTwapEvent)
        )
        assert persisted_event.received_wall_ns > 0
        assert persisted_event.received_monotonic_ns > 0

    asyncio.run(scenario())


def test_reader_attempts_twap_live_cache_before_postgres_queue(monkeypatch):
    async def scenario():
        order = []
        message = current_twap_message()
        websocket = OneMessageWebSocket(
            json.dumps(message, default=str),
            [],
        )

        class RecordingQueue(AcknowledgingQueue):
            def put_nowait(self, item):
                if isinstance(item, twap.PolymarketTwapEvent):
                    order.append("postgres_queue")
                return super().put_nowait(item)

        class RecordingLiveCache:
            async def set_price(self, key, **fields):
                order.append("redis")
                assert key == "btc:live:chainlink_twap_60s"
                assert fields == {
                    "value": Decimal("64255.113422936400000000"),
                    "source_timestamp_ms": message["payload"]["timestamp"],
                    "received_ms": fields["received_ms"],
                }

        records = RecordingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
                live_cache=RecordingLiveCache(),
            )
        )
        while "postgres_queue" not in order:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert order[:2] == ["redis", "postgres_queue"]

    asyncio.run(scenario())




def test_redis_failure_does_not_discard_twap_postgres_event(monkeypatch, caplog):
    async def scenario():
        websocket = OneMessageWebSocket(
            json.dumps(current_twap_message(), default=str),
            [],
        )

        class FailingLiveCache:
            async def set_price(self, *args, **kwargs):
                raise OSError("Redis unavailable")

        records = AcknowledgingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
                live_cache=FailingLiveCache(),
            )
        )
        while not any(
            isinstance(row, twap.PolymarketTwapEvent)
            for row in records._queue
        ):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert "polymarket_twap_live_cache_write_failed" in caplog.text
        assert any(
            isinstance(row, twap.PolymarketTwapEvent)
            for row in records._queue
        )

    asyncio.run(scenario())


def test_cancellation_during_redis_preserves_event_before_planned_gap(monkeypatch):
    async def scenario():
        order = []
        redis_entered = asyncio.Event()
        release_redis = asyncio.Event()
        websocket = OneMessageWebSocket(
            json.dumps(current_twap_message(), default=str),
            [],
        )

        class RecordingQueue(AcknowledgingQueue):
            def put_nowait(self, item):
                if isinstance(item, twap.PolymarketTwapEvent):
                    order.append("event_queued")
                elif isinstance(item, twap.PolymarketTwapGap):
                    order.append("gap_queued")
                elif isinstance(item, twap.PolymarketTwapSessionFinish):
                    order.append("finish_queued")
                return super().put_nowait(item)

        class BlockingLiveCache:
            async def set_price(self, *args, **kwargs):
                order.append("redis_started")
                redis_entered.set()
                await release_redis.wait()
                order.append("redis_finished")

        records = RecordingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        reader = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
                live_cache=BlockingLiveCache(),
            )
        )
        await asyncio.wait_for(redis_entered.wait(), timeout=1)

        reader.cancel()
        await asyncio.sleep(0)
        assert "event_queued" not in order
        release_redis.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(reader, timeout=1)

        assert order.index("redis_finished") < order.index("event_queued")
        assert order.index("event_queued") < order.index("gap_queued")
        assert order.index("gap_queued") < order.index("finish_queued")
        queued_event = next(
            row
            for row in records._queue
            if isinstance(row, twap.PolymarketTwapEvent)
        )
        gap = next(
            row for row in records._queue if isinstance(row, twap.PolymarketTwapGap)
        )
        assert gap.reason == "planned_restart"
        assert gap.last_provider_event_ms == queued_event.provider_event_ms

    asyncio.run(scenario())


class NonAcceptedFramesWebSocket:
    def __init__(self):
        self.sent = []
        self.index = 0

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.002)
        frames = (
            "PONG",
            "not-json",
            json.dumps(
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "payload": {"symbol": "btc/usd"},
                }
            ),
            json.dumps(valid_twap_message(window_s=30), default=str),
            json.dumps(
                {
                    **valid_twap_message(),
                    "topic": "crypto_prices_twap_thirty",
                },
                default=str,
            ),
        )
        frame = frames[self.index % len(frames)]
        self.index += 1
        return frame


class RepeatingFrameWebSocket:
    def __init__(self, frame):
        self.frame = frame
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(0.002)
        return self.frame


class SubscriptionFailureWebSocket:
    def __init__(self):
        self.failed = asyncio.Event()

    async def send(self, message):
        self.failed.set()
        raise OSError("subscription send failed")


def test_subscription_failure_does_not_queue_orphan_gap_without_session(
    monkeypatch,
):
    async def scenario():
        websocket = SubscriptionFailureWebSocket()
        records = AcknowledgingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 60)
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
            )
        )

        await asyncio.wait_for(websocket.failed.wait(), timeout=1)
        await asyncio.sleep(0.01)
        assert records.empty()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_idle_deadline_ignores_ping_malformed_and_unrelated_frames(monkeypatch):
    async def scenario():
        websocket = NonAcceptedFramesWebSocket()
        records = AcknowledgingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 60)

        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(idle_timeout_ms=100),
                instrument_id=77,
                records=records,
            )
        )
        while not any(isinstance(row, twap.PolymarketTwapGap) for row in records._queue):
            await asyncio.sleep(0.001)

        gap = next(
            row for row in records._queue if isinstance(row, twap.PolymarketTwapGap)
        )
        finish = next(
            row
            for row in records._queue
            if isinstance(row, twap.PolymarketTwapSessionFinish)
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert gap.reason == "accepted_event_idle_timeout"
        assert gap.idle_timeout_ms == 100
        assert gap.messages_received_total > 0
        assert gap.messages_accepted_total == 0
        assert gap.parse_errors_total > 0
        assert finish.close_reason == "accepted_event_idle_timeout"

    asyncio.run(scenario())


def test_empty_ack_and_control_frames_do_not_reset_idle_or_count_as_parse_errors(
    monkeypatch,
):
    async def scenario():
        websocket = RepeatingFrameWebSocket("")
        records = AcknowledgingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 60)
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(idle_timeout_ms=30),
                instrument_id=77,
                records=records,
            )
        )
        while not any(
            isinstance(row, twap.PolymarketTwapGap)
            for row in records._queue
        ):
            await asyncio.sleep(0.001)

        gap = next(
            row for row in records._queue if isinstance(row, twap.PolymarketTwapGap)
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert gap.reason == "accepted_event_idle_timeout"
        assert gap.messages_accepted_total == 0
        assert gap.parse_errors_total == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("source_offset_ms", (-100, 3_000))
def test_stale_and_future_ticks_do_not_reset_idle_publish_or_persist(
    monkeypatch,
    source_offset_ms,
):
    async def scenario():
        now_ms = twap.time.time_ns() // 1_000_000
        message = valid_twap_message(timestamp=now_ms + source_offset_ms)
        message["timestamp"] = now_ms
        websocket = RepeatingFrameWebSocket(json.dumps(message, default=str))
        records = AcknowledgingQueue(maxsize=103)

        class RejectLiveCache:
            async def set_price(self, *args, **kwargs):
                pytest.fail("implausible TWAP tick reached Redis")

        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 60)
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(idle_timeout_ms=40),
                instrument_id=77,
                records=records,
                live_cache=RejectLiveCache(),
            )
        )
        while not any(
            isinstance(row, twap.PolymarketTwapGap)
            for row in records._queue
        ):
            await asyncio.sleep(0.001)

        gap = next(
            row for row in records._queue if isinstance(row, twap.PolymarketTwapGap)
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert gap.messages_accepted_total == 0
        assert gap.parse_errors_total > 0
        assert not any(
            isinstance(row, twap.PolymarketTwapEvent)
            for row in records._queue
        )

    asyncio.run(scenario())


class ClosingWebSocket:
    def __init__(self):
        self.closed = asyncio.Event()
        self.close_args = None

    async def close(self, **kwargs):
        self.close_args = kwargs
        self.closed.set()


def test_queue_saturation_preserves_current_event_and_forces_explicit_gap(caplog):
    async def scenario():
        records = asyncio.Queue(maxsize=4)
        records.put_nowait(
            twap.PolymarketTwapSessionStart(
                connection_id=UUID("22222222-2222-2222-2222-222222222222"),
                topic="crypto_prices_twap_sixty",
                symbol="btc/usd",
                window_s=60,
                connected_wall_ns=1,
                connected_monotonic_ns=1,
                subscribed_wall_ns=2,
                subscribed_monotonic_ns=2,
            )
        )
        event = twap_event()
        websocket = ClosingWebSocket()
        offer = asyncio.create_task(
            twap._enqueue_twap_event(
                records,
                event,
                websocket=websocket,
                event_capacity=1,
            )
        )

        await asyncio.wait_for(websocket.closed.wait(), timeout=1)
        with pytest.raises(twap.RtdsTwapPersistenceBackpressure):
            await asyncio.wait_for(offer, timeout=1)
        queued = list(records._queue)
        assert isinstance(queued[0], twap.PolymarketTwapSessionStart)
        assert queued[1] == event
        assert websocket.close_args["code"] == 1013
        assert "polymarket_twap_persistence_queue_saturated" in caplog.text

    asyncio.run(scenario())


def test_independent_persistence_worker_retries_exact_event(monkeypatch):
    async def scenario():
        calls = []

        async def flaky_record(pool, **kwargs):
            calls.append((pool, kwargs))
            if len(calls) == 1:
                raise RuntimeError("temporary database delay")

        monkeypatch.setattr(
            twap.db,
            "record_polymarket_twap_event",
            flaky_record,
            raising=False,
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 0)
        records = asyncio.Queue(maxsize=10)
        event = twap_event(receive_sequence=9)
        records.put_nowait(event)
        worker = asyncio.create_task(
            twap.polymarket_twap_persistence_worker(
                pool="pool",
                records=records,
            )
        )

        await asyncio.wait_for(records.join(), timeout=1)
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

        assert len(calls) == 2
        kwargs = calls[-1][1]
        assert kwargs["connection_id"] == event.connection_id
        assert kwargs["receive_sequence"] == 9
        assert kwargs["topic"] == "crypto_prices_twap_sixty"
        assert kwargs["window_s"] == 60
        assert kwargs["price_e18"] == 64_255_113_422_936_400_000_000
        assert kwargs["price"] == Decimal("64255.113422936400000000")
        assert kwargs["provider_event_ms"] == 1_786_665_600_123
        assert kwargs["received_wall_ns"] == 1_786_665_600_500_123_456
        assert kwargs["received_monotonic_ns"] == 9_000_000_000
        assert kwargs["sample_second_ms"] == 1_786_665_600_000

    asyncio.run(scenario())


def test_reader_waits_for_durable_session_commit_before_recv(monkeypatch):
    async def scenario():
        order = []
        start_entered = asyncio.Event()
        allow_start_commit = asyncio.Event()
        recv_called = asyncio.Event()
        event_persisted = asyncio.Event()
        start_attempts = 0

        class BarrierWebSocket(OneMessageWebSocket):
            async def __anext__(self):
                order.append("recv")
                recv_called.set()
                return await super().__anext__()

        websocket = BarrierWebSocket(
            json.dumps(current_twap_message(), default=str),
            [],
        )

        async def persist_start(pool, **kwargs):
            nonlocal start_attempts
            start_attempts += 1
            order.append("start_begin")
            if start_attempts == 1:
                raise RuntimeError("temporary session-start failure")
            start_entered.set()
            await allow_start_commit.wait()
            order.append("start_commit")

        async def persist_event(pool, **kwargs):
            order.append("event_commit")
            event_persisted.set()

        async def persist_gap(pool, **kwargs):
            order.append("gap_commit")

        async def persist_finish(pool, **kwargs):
            order.append("finish_commit")

        monkeypatch.setattr(twap.db, "start_polymarket_twap_session", persist_start)
        monkeypatch.setattr(twap.db, "record_polymarket_twap_event", persist_event)
        monkeypatch.setattr(twap.db, "record_polymarket_twap_gap", persist_gap)
        monkeypatch.setattr(
            twap.db,
            "finish_polymarket_twap_session",
            persist_finish,
        )
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 0)
        records = asyncio.Queue(maxsize=103)
        writer = asyncio.create_task(
            twap.polymarket_twap_persistence_worker(
                pool="pool",
                records=records,
            )
        )
        reader = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
            )
        )

        await asyncio.wait_for(start_entered.wait(), timeout=1)
        await asyncio.sleep(0.01)
        assert not recv_called.is_set()
        assert start_attempts == 2
        allow_start_commit.set()
        await asyncio.wait_for(recv_called.wait(), timeout=1)
        await asyncio.wait_for(event_persisted.wait(), timeout=1)
        assert order.index("start_commit") < order.index("recv")
        assert order.index("start_commit") < order.index("event_commit")

        reader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reader
        await asyncio.wait_for(records.join(), timeout=1)
        writer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer

    asyncio.run(scenario())


def test_planned_cancellation_queues_gap_before_session_finish(monkeypatch):
    async def scenario():
        websocket = OneMessageWebSocket(
            json.dumps(current_twap_message(), default=str),
            [],
        )
        records = AcknowledgingQueue(maxsize=103)
        monkeypatch.setattr(
            twap.websockets,
            "connect",
            lambda *args, **kwargs: WebSocketContext(websocket),
        )
        task = asyncio.create_task(
            twap.polymarket_twap_reader_loop(
                twap_settings(),
                instrument_id=77,
                records=records,
            )
        )
        while not any(
            isinstance(row, twap.PolymarketTwapEvent)
            for row in records._queue
        ):
            await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        queued = list(records._queue)
        gap_index = next(
            index
            for index, row in enumerate(queued)
            if isinstance(row, twap.PolymarketTwapGap)
        )
        finish_index = next(
            index
            for index, row in enumerate(queued)
            if isinstance(row, twap.PolymarketTwapSessionFinish)
        )
        gap = queued[gap_index]
        finish = queued[finish_index]
        assert gap.reason == "planned_restart"
        assert finish.close_reason == "planned_restart"
        assert gap_index < finish_index

    asyncio.run(scenario())


def test_final_control_offer_is_nonblocking_when_reserve_is_exhausted(caplog):
    async def scenario():
        records = asyncio.Queue(maxsize=1)
        records.put_nowait(twap_event())

        offered = twap._offer_twap_session_final_records(
            records,
            connection_id=UUID("33333333-3333-3333-3333-333333333333"),
            close_reason="planned_restart",
            gap_reason="planned_restart",
            gap_idle_timeout_ms=None,
            messages_received_total=1,
            messages_accepted_total=1,
            parse_errors_total=0,
            receive_sequence=1,
            last_accepted_received_ms=1_786_665_600_500,
            last_provider_event_ms=1_786_665_600_123,
        )

        assert offered is False
        assert records.qsize() == 1

    asyncio.run(scenario())
    assert "polymarket_twap_final_control_not_enqueued" in caplog.text


def test_startup_recovers_open_session_as_gap_before_finish(monkeypatch):
    async def scenario():
        connection_id = UUID("44444444-4444-4444-4444-444444444444")
        queries = []
        calls = []

        class Connection:
            async def fetch(self, query, *args):
                queries.append((" ".join(query.split()), args))
                return [
                    {
                        "connection_id": connection_id,
                        "subscribed_monotonic_ns": 8_000_000_000_000_000_000,
                        "messages_received_total": 9,
                        "messages_accepted_total": 4,
                        "parse_errors_total": 2,
                        "last_receive_sequence": 9,
                        "last_accepted_received_ms": 1_786_665_600_500,
                        "last_provider_event_ms": 1_786_665_600_123,
                    }
                ]

        class Acquire:
            async def __aenter__(self):
                return Connection()

            async def __aexit__(self, exc_type, exc, traceback):
                return False

        class Pool:
            def acquire(self):
                return Acquire()

        async def record_gap(pool, **kwargs):
            calls.append(("gap", kwargs))

        async def finish_session(pool, **kwargs):
            calls.append(("finish", kwargs))

        monkeypatch.setattr(twap.db, "record_polymarket_twap_gap", record_gap)
        monkeypatch.setattr(
            twap.db,
            "finish_polymarket_twap_session",
            finish_session,
        )

        recovered = await twap.recover_orphaned_polymarket_twap_sessions(Pool())

        assert recovered == 1
        assert [kind for kind, _kwargs in calls] == ["gap", "finish"]
        assert calls[0][1]["reason"] == "orphaned_session_recovered"
        assert calls[1][1]["close_reason"] == "orphaned_session_recovered"
        assert calls[0][1]["detected_wall_ns"] == calls[1][1][
            "disconnected_wall_ns"
        ]
        assert (
            calls[1][1]["disconnected_monotonic_ns"]
            >= 8_000_000_000_000_000_000
        )
        assert calls[0][1]["messages_accepted_total"] == 4
        query, args = queries[0]
        assert "session.disconnected_wall_ns IS NULL" in query
        assert "session.symbol = $1::TEXT" in query
        assert "session.topic = $2::TEXT" in query
        assert "session.window_s = $3::SMALLINT" in query
        assert "session.topic = $4::TEXT" in query
        assert "session.window_s = $5::SMALLINT" in query
        assert args == (
            "btc/usd",
            "crypto_prices_twap_thirty",
            30,
            "crypto_prices_twap_sixty",
            60,
        )

    asyncio.run(scenario())


def test_runtime_bounds_cancellation_resistant_reader_shutdown(monkeypatch):
    async def scenario():
        reader_started = asyncio.Event()
        first_cancel_seen = asyncio.Event()

        async def no_orphans(pool):
            return 0

        async def instrument_id(pool, *, provider_code, symbol):
            return 77

        async def resistant_reader(*args, **kwargs):
            reader_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                first_cancel_seen.set()
                await asyncio.Event().wait()

        monkeypatch.setattr(
            twap,
            "recover_orphaned_polymarket_twap_sessions",
            no_orphans,
        )
        monkeypatch.setattr(twap.db, "get_instrument_id", instrument_id)
        monkeypatch.setattr(twap, "polymarket_twap_reader_loop", resistant_reader)
        settings = twap_settings()
        settings.POLYMARKET_TWAP_PERSIST_SHUTDOWN_TIMEOUT_SECONDS = 0.05
        runtime = asyncio.create_task(
            twap.run_polymarket_twap_runtime(settings, "pool")
        )
        await asyncio.wait_for(reader_started.wait(), timeout=1)

        started = asyncio.get_running_loop().time()
        runtime.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(runtime, timeout=0.5)
        elapsed = asyncio.get_running_loop().time() - started

        assert first_cancel_seen.is_set()
        assert elapsed < 0.5

    asyncio.run(scenario())


def test_noncritical_supervisor_restarts_without_completing(monkeypatch):
    async def scenario():
        runtime_calls = 0
        restarted = asyncio.Event()

        async def fake_runtime(settings, pool, *, live_cache=None):
            nonlocal runtime_calls
            runtime_calls += 1
            if runtime_calls == 1:
                raise RuntimeError("TWAP-only failure")
            restarted.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(twap, "run_polymarket_twap_runtime", fake_runtime)
        monkeypatch.setattr(twap, "reconnect_delay_seconds", lambda _attempt: 0)
        task = asyncio.create_task(
            twap.run_polymarket_twap_noncritical(twap_settings(), "pool")
        )

        await asyncio.wait_for(restarted.wait(), timeout=1)
        assert runtime_calls == 2
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
