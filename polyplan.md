

# Updated blueprint: add Polymarket Chainlink RTDS source

## 1. Critical source distinction

For Polymarket, do **not** subscribe to this:

```json
{
  "topic": "crypto_prices",
  "filters": "btcusdt"
}
```

That is the **Binance-sourced Polymarket RTDS feed**.

Subscribe only to this:

```json
{
  "topic": "crypto_prices_chainlink",
  "type": "*",
  "filters": "{\"symbol\":\"btc/usd\"}"
}
```

Polymarket’s RTDS documentation says the RTDS endpoint is:

```text
wss://ws-live-data.polymarket.com
```

It also documents two crypto price sources: `crypto_prices` for Binance-style symbols like `btcusdt`, and `crypto_prices_chainlink` for Chainlink-style symbols like `btc/usd`. Polymarket’s Chainlink BTC update example uses topic `crypto_prices_chainlink`, payload symbol `btc/usd`, payload timestamp, and payload value. ([Polymarket Documentation][1])

This matters because Polymarket’s own BTC 5-minute market rules say the resolution source is Chainlink BTC/USD Data Stream, and specifically not other spot-market prices. ([Polymarket][2])

---

## 2. Do not use direct Chainlink WebSocket

The agent should **not** connect directly to:

```text
wss://ws.dataengine.chain.link
```

Direct Chainlink Data Streams WebSocket connections require authentication headers/signatures. ([Chainlink Documentation][3])

Use Polymarket’s RTDS endpoint instead:

```text
wss://ws-live-data.polymarket.com
```

That gives the Chainlink BTC/USD RTDS values Polymarket exposes.

---

## 3. Provider model

Add a second provider/instrument.

Current source:

```text
provider_code: binance_spot
symbol: BTCUSDT
stream: wss://stream.binance.com:9443/ws/btcusdt@ticker
price field: c
timestamp field: E
quote: USDT
```

New source:

```text
provider_code: polymarket_chainlink_rtds
symbol: BTCUSD
stream endpoint: wss://ws-live-data.polymarket.com
RTDS topic: crypto_prices_chainlink
RTDS filter symbol: btc/usd
price field: payload.value
timestamp field: payload.timestamp
quote: USD
```

Use `BTCUSD` in your own DB because the source is BTC/USD, not BTC/USDT.

---

## 4. Important timing rule for Polymarket Chainlink

For Binance, your current collector samples once per local UTC second and stores Binance event time separately. Your current code computes `sample_second_ms` from current time and then maps that to a 5-minute market window. 

For **Polymarket Chainlink RTDS**, the agent should not invent repeated samples from stale data. Instead, it should write **one row per source timestamp second**:

```python
provider_event_ms = int(payload["timestamp"])
sample_second_ms = (provider_event_ms // 1000) * 1000
```

Then apply your same market-window rule:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

Reason: near a 5-minute boundary, the Chainlink/Polymarket payload timestamp is more important than your droplet’s receive time. If Polymarket sends a tick late, you still want to classify it by the source timestamp.

Store this too:

```python
received_ms = current_utc_epoch_ms()
```

So each row has:

```text
sample_second_ms: source timestamp floored to whole second
provider_event_ms: exact Polymarket/Chainlink payload timestamp
received_ms: when your droplet received it
```

---

## 5. Database migration

No major schema change is required if your existing tables are the same as before. The current DB layer already finds instruments by `provider_code` and `symbol`, and writes samples using `instrument_id`, `sample_second_ms`, `market_id`, `price`, `provider_event_ms`, and `received_ms`. 

Add this seed migration:

```sql
INSERT INTO providers (provider_code, display_name)
VALUES ('polymarket_chainlink_rtds', 'Polymarket RTDS Chainlink BTC/USD')
ON CONFLICT (provider_code) DO NOTHING;

INSERT INTO instruments (
    provider_id,
    symbol,
    base_asset,
    quote_asset,
    stream_name
)
SELECT
    provider_id,
    'BTCUSD',
    'BTC',
    'USD',
    'crypto_prices_chainlink:btc/usd'
FROM providers
WHERE provider_code = 'polymarket_chainlink_rtds'
ON CONFLICT (provider_id, symbol) DO NOTHING;
```

Optional but useful migration:

```sql
ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS provider_message_ms BIGINT;

ALTER TABLE price_samples
ADD COLUMN IF NOT EXISTS source_topic TEXT;
```

If the agent adds these columns, then Polymarket rows can store:

```text
provider_event_ms = payload.timestamp
provider_message_ms = top-level message timestamp
source_topic = crypto_prices_chainlink
source_price_field = payload.value
```

If the agent does not add those columns, still store:

```text
provider_event_ms = payload.timestamp
source_price_field = payload.value
```

---

## 6. New collector file

Add:

```text
price_collector/polymarket_chainlink_collector.py
```

Do **not** replace the Binance collector yet. Add a second service.

The new collector should:

1. Connect to `wss://ws-live-data.polymarket.com`.
2. Send the Chainlink subscription message.
3. Send text `PING` every 5 seconds.
4. Parse only `crypto_prices_chainlink` messages.
5. Accept only payload symbol `btc/usd`.
6. Use `payload.value` as price.
7. Use `payload.timestamp` as source event time.
8. Write at most one row per source timestamp second.
9. Reconnect automatically with exponential backoff and jitter.
10. Never connect directly to Chainlink paid/authenticated WebSocket.

Polymarket’s RTDS docs say to send `PING` messages every 5 seconds to maintain the connection. ([Polymarket Documentation][1])

---

## 7. Subscription message

The agent should send exactly this after connecting:

```python
subscription = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}",
        }
    ],
}
```

Send it as JSON:

```python
await websocket.send(json.dumps(subscription))
```

Do not use:

```python
"topic": "crypto_prices"
```

Do not use:

```python
"filters": "btcusdt"
```

---

## 8. Parser requirements

Add a parser like this:

```python
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


class RtdsParseError(ValueError):
    pass


@dataclass(frozen=True)
class PolymarketChainlinkTick:
    symbol: str
    price: Decimal
    provider_event_ms: int
    provider_message_ms: int | None


def parse_polymarket_chainlink_message(
    message: Mapping[str, Any],
    *,
    expected_symbol: str = "btc/usd",
) -> PolymarketChainlinkTick:
    topic = message.get("topic")
    if topic != "crypto_prices_chainlink":
        raise RtdsParseError(
            f"unexpected RTDS topic: expected 'crypto_prices_chainlink', got {topic!r}"
        )

    message_type = message.get("type")
    if message_type != "update":
        raise RtdsParseError(f"non-update RTDS message: {message_type!r}")

    payload = message.get("payload")
    if not isinstance(payload, Mapping):
        raise RtdsParseError("RTDS message payload must be an object")

    symbol = payload.get("symbol")
    if symbol != expected_symbol:
        raise RtdsParseError(
            f"unexpected Chainlink symbol: expected {expected_symbol!r}, got {symbol!r}"
        )

    raw_value = payload.get("value")
    if raw_value is None:
        raise RtdsParseError("RTDS payload missing price field payload.value")

    try:
        price = raw_value if isinstance(raw_value, Decimal) else Decimal(str(raw_value))
    except (InvalidOperation, ValueError) as exc:
        raise RtdsParseError("invalid RTDS price field payload.value") from exc

    if not price.is_finite() or price <= 0:
        raise RtdsParseError("RTDS price must be finite and positive")

    raw_event_ms = payload.get("timestamp")
    try:
        provider_event_ms = int(raw_event_ms)
    except (TypeError, ValueError) as exc:
        raise RtdsParseError("invalid RTDS payload.timestamp") from exc

    if provider_event_ms <= 0:
        raise RtdsParseError("RTDS payload.timestamp must be positive")

    raw_message_ms = message.get("timestamp")
    provider_message_ms = None
    if raw_message_ms is not None:
        try:
            provider_message_ms = int(raw_message_ms)
        except (TypeError, ValueError):
            provider_message_ms = None

    return PolymarketChainlinkTick(
        symbol="BTCUSD",
        price=price,
        provider_event_ms=provider_event_ms,
        provider_message_ms=provider_message_ms,
    )
```

Use JSON parsing like this so numeric prices do not become binary floats:

```python
payload = json.loads(message, parse_float=Decimal)
```

---

## 9. Polymarket collector write rule

Unlike the Binance collector, the Polymarket collector can write on each valid RTDS update because the feed is already roughly one update per second. But it must still dedupe by source timestamp second.

Pseudo-code:

```python
async def handle_tick(pool, instrument_id: int, tick: PolymarketChainlinkTick) -> None:
    sample_second_ms = (tick.provider_event_ms // 1000) * 1000
    window = market_for_sample_second(sample_second_ms)
    received_ms = current_utc_epoch_ms()

    await upsert_price_sample(
        pool,
        instrument_id=instrument_id,
        sample_second_ms=sample_second_ms,
        window=window,
        price=tick.price,
        provider_event_ms=tick.provider_event_ms,
        received_ms=received_ms,
        source_price_field="payload.value",
    )
```

Because your existing `upsert_price_sample` does `ON CONFLICT (instrument_id, sample_second_ms) DO UPDATE`, duplicate ticks in the same source second will update, not create duplicate rows. 

---

## 10. WebSocket loop skeleton

```python
async def rtds_ping_loop(websocket) -> None:
    while True:
        await asyncio.sleep(5)
        await websocket.send("PING")


async def polymarket_chainlink_reader_loop(settings: Settings, pool, instrument_id: int) -> None:
    attempt = 0

    while True:
        try:
            async with websockets.connect(
                settings.POLYMARKET_RTDS_WS_URL,
                ping_interval=None,
                close_timeout=10,
            ) as websocket:
                attempt = 0

                await websocket.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "{\"symbol\":\"btc/usd\"}",
                        }
                    ],
                }))

                ping_task = asyncio.create_task(rtds_ping_loop(websocket))

                try:
                    async for raw_message in websocket:
                        if raw_message in ("PONG", "PING"):
                            continue

                        try:
                            message = json.loads(raw_message, parse_float=Decimal)
                            tick = parse_polymarket_chainlink_message(
                                message,
                                expected_symbol=settings.POLYMARKET_CHAINLINK_RTD_SYMBOL,
                            )
                        except (json.JSONDecodeError, RtdsParseError) as exc:
                            LOGGER.warning(
                                "polymarket_rtds_message_skipped",
                                extra={
                                    "event": "polymarket_rtds_message_skipped",
                                    "error": str(exc),
                                },
                            )
                            continue

                        await handle_tick(pool, instrument_id, tick)

                finally:
                    ping_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            attempt += 1
            delay = reconnect_delay_seconds(attempt)
            LOGGER.warning(
                "polymarket_rtds_reconnect_scheduled",
                extra={
                    "event": "polymarket_rtds_reconnect_scheduled",
                    "attempt": attempt,
                    "delay_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await asyncio.sleep(delay)
```

The agent can reuse your existing `reconnect_delay_seconds`, `current_utc_epoch_ms`, `market_for_sample_second`, `get_instrument_id`, and `upsert_price_sample`.

---

## 11. Config changes

Update `config.py`:

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=True)

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    # Existing Binance settings
    BINANCE_WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    PROVIDER_CODE: str = "binance_spot"
    SYMBOL: str = "BTCUSDT"
    STALE_PRICE_MS: int = 10_000

    # New Polymarket Chainlink RTDS settings
    POLYMARKET_RTDS_WS_URL: str = "wss://ws-live-data.polymarket.com"
    POLYMARKET_CHAINLINK_PROVIDER_CODE: str = "polymarket_chainlink_rtds"
    POLYMARKET_CHAINLINK_SYMBOL: str = "BTCUSD"
    POLYMARKET_CHAINLINK_RTD_SYMBOL: str = "btc/usd"
    POLYMARKET_CHAINLINK_TOPIC: str = "crypto_prices_chainlink"

    DATABASE_URL: Optional[str] = None
    READ_DATABASE_URL: Optional[str] = None
```

---

## 12. systemd service

Keep your existing Binance service.

Add a second service:

```bash
sudo nano /etc/systemd/system/price-collector-polymarket-chainlink.service
```

```ini
[Unit]
Description=Polymarket Chainlink BTC/USD RTDS price collector
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=pricecollector
Group=pricecollector
WorkingDirectory=/opt/price-collector
EnvironmentFile=/etc/price-collector/price-collector.env
ExecStart=/opt/price-collector/.venv/bin/python -m price_collector.polymarket_chainlink_collector

Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/price-collector

[Install]
WantedBy=multi-user.target
```

Enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable price-collector-polymarket-chainlink
sudo systemctl start price-collector-polymarket-chainlink
```

Check logs:

```bash
sudo journalctl -u price-collector-polymarket-chainlink -f
```

---

## 13. API behavior

Your current API can already query a different provider because `/prices/latest`, `/markets/latest`, and `/markets/{market_id}` accept `provider` and `symbol` query parameters. 

So after adding the DB seed and the Polymarket collector, these should work:

```bash
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"

curl "http://127.0.0.1:9000/markets/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
```

Keep the existing Binance calls:

```bash
curl "http://127.0.0.1:9000/prices/latest?provider=binance_spot&symbol=BTCUSDT"

curl "http://127.0.0.1:9000/markets/latest?provider=binance_spot&symbol=BTCUSDT"
```

---

## 14. Recommended combined market endpoint

Add a new endpoint so your dashboard can get both sources for the same 5-minute market.

Add:

```text
GET /markets/current/sources
GET /markets/{market_id}/sources
```

Example response:

```json
{
  "market_id": 5944864,
  "market_start_ms": 1783459200000,
  "market_end_ms": 1783459500000,
  "market_start_at": "2026-07-07T21:00:00Z",
  "market_end_at": "2026-07-07T21:05:00Z",
  "is_complete": false,
  "sources": [
    {
      "provider": "binance_spot",
      "symbol": "BTCUSDT",
      "quote_asset": "USDT",
      "sample_count": 300,
      "open": "123000.00000000",
      "high": "123500.00000000",
      "low": "122900.00000000",
      "close": "123456.78000000",
      "latest_sample_second_ms": 1783459499000,
      "latest_provider_event_ms": 1783459498950,
      "latest_received_ms": 1783459499010
    },
    {
      "provider": "polymarket_chainlink_rtds",
      "symbol": "BTCUSD",
      "quote_asset": "USD",
      "sample_count": 298,
      "open": "122998.12000000",
      "high": "123501.99000000",
      "low": "122901.03000000",
      "close": "123455.90000000",
      "latest_sample_second_ms": 1783459499000,
      "latest_provider_event_ms": 1783459499123,
      "latest_received_ms": 1783459499320
    }
  ]
}
```

Important: `sample_count` for Polymarket may be less than 300 if the RTDS feed has gaps. Do not fill missing seconds with stale carried-forward prices unless you explicitly add a separate derived table later.

---

## 15. DB helper for combined endpoint

Add a DB function:

```python
async def fetch_market_summaries_for_all_btc_sources(
    pool: asyncpg.Pool,
    market_id: int,
) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT
                p.provider_code AS provider,
                i.symbol AS symbol,
                i.quote_asset AS quote_asset,
                mw.market_id AS market_id,
                mw.market_start_ms AS market_start_ms,
                mw.market_end_ms AS market_end_ms,
                mw.market_start_at AS market_start_at,
                mw.market_end_at AS market_end_at,
                ps.sample_second_ms AS sample_second_ms,
                ps.sample_second_at AS sample_second_at,
                ps.price AS price,
                ps.provider_event_ms AS provider_event_ms,
                ps.received_ms AS received_ms
            FROM price_samples ps
            JOIN instruments i ON i.instrument_id = ps.instrument_id
            JOIN providers p ON p.provider_id = i.provider_id
            JOIN market_windows mw ON mw.market_id = ps.market_id
            WHERE ps.market_id = $1
              AND i.base_asset = 'BTC'
              AND p.provider_code IN ('binance_spot', 'polymarket_chainlink_rtds')
            ORDER BY p.provider_code ASC, ps.sample_second_ms ASC
            """,
            market_id,
        )

    # Group rows by provider+symbol and compute open/high/low/close.
```

Keep your old single-provider API functions unchanged.

---

## 16. Verification SQL

After both collectors run for a few minutes:

```sql
SELECT
    p.provider_code,
    i.symbol,
    i.quote_asset,
    count(*) AS rows,
    min(ps.sample_second_at) AS first_sample,
    max(ps.sample_second_at) AS latest_sample
FROM price_samples ps
JOIN instruments i ON i.instrument_id = ps.instrument_id
JOIN providers p ON p.provider_id = i.provider_id
GROUP BY p.provider_code, i.symbol, i.quote_asset
ORDER BY p.provider_code, i.symbol;
```

Expected:

```text
binance_spot                 BTCUSDT  USDT  ...
polymarket_chainlink_rtds    BTCUSD   USD   ...
```

Check the latest shared markets:

```sql
SELECT
    ps.market_id,
    p.provider_code,
    i.symbol,
    count(*) AS sample_count,
    min(ps.sample_second_ms) AS first_second_ms,
    max(ps.sample_second_ms) AS last_second_ms,
    min(ps.price) AS low,
    max(ps.price) AS high
FROM price_samples ps
JOIN instruments i ON i.instrument_id = ps.instrument_id
JOIN providers p ON p.provider_id = i.provider_id
WHERE p.provider_code IN ('binance_spot', 'polymarket_chainlink_rtds')
GROUP BY ps.market_id, p.provider_code, i.symbol
ORDER BY ps.market_id DESC, p.provider_code
LIMIT 20;
```

---

## 17. Agent acceptance tests

The agent should add tests for:

```text
1. Polymarket subscription uses topic crypto_prices_chainlink.
2. Polymarket subscription filter is exactly {"symbol":"btc/usd"}.
3. Parser rejects topic crypto_prices.
4. Parser rejects symbol btcusdt.
5. Parser accepts topic crypto_prices_chainlink and symbol btc/usd.
6. Parser uses payload.value as Decimal.
7. Parser uses payload.timestamp as provider_event_ms.
8. sample_second_ms is provider_event_ms floored to whole second.
9. market_id uses the same 300_000 ms formula.
10. Exact 5-minute boundary belongs to the new market.
11. Duplicate Polymarket ticks in the same source second do not create duplicate DB rows.
12. API can query provider=polymarket_chainlink_rtds&symbol=BTCUSD.
13. Combined endpoint returns both Binance and Polymarket Chainlink for the same market_id.
14. Code never connects to wss://ws.dataengine.chain.link.
15. Code never subscribes to Polymarket topic crypto_prices for the Chainlink collector.
```

---


This gives you two independent sources in the same 5-minute market grid: Binance Spot `BTCUSDT` and Polymarket’s Chainlink-backed `BTCUSD` RTDS feed.

[1]: https://docs.polymarket.com/market-data/websocket/rtds "Real-Time Data Socket - Polymarket Documentation"
[2]: https://polymarket.com/event/btc-updown-5m-1773371400 "BTC Up or Down 5m Predictions & Odds 2026 | Polymarket"
[3]: https://docs.chain.link/data-streams/reference/data-streams-api/interface-ws "Data Streams WebSocket | Chainlink Documentation"
