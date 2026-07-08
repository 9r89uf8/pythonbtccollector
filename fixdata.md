The two problems are separate:

1. **Missing first 5–15 seconds** happens because the probability collector waits until the old market ends, then discovers the new Gamma market, connects to CLOB, subscribes, waits for book/best-bid-ask data, and only then starts writing. Your uploaded JSON confirms this: the 12:50:00 market has `null` probabilities at `t=0` through the early seconds, and the first real probability appears around `t=11`. 

2. **Too many decimals / too many probability fields** is just a JSON serialization issue. Your current download builder uses `decimal_or_none(...)`, which formats Decimals with full stored precision, and it explicitly exports `bid`, `ask`, `mid`, and `normalized`. 


---

# Codex instructions: fix early Polymarket probability gaps and compact JSON export

## Goal

Modify the existing `price_collector` project.

Fix two things:

```text
1. Probability collector should be ready before each 5-minute market starts,
   so we do not miss the first 5–15 seconds.

2. Download JSON should output:
   - BTC prices with exactly 2 decimals.
   - Probability data with ask only.
   - Probability ask values with exactly 2 decimals.
```

Desired JSON shape:

```json
{
  "t": 12,
  "timestamp_ms": 1783515012000,
  "timestamp_at": "2026-07-08T12:50:12Z",
  "prices": {
    "binance": "62067.90",
    "chainlink": "62013.53"
  },
  "probabilities": {
    "up": {
      "ask": "0.56"
    },
    "down": {
      "ask": "0.45"
    }
  }
}
```

Keep using Decimal internally. Do **not** convert prices/probabilities to Python float.

---

## Part 1: fix JSON formatting

### 1. Replace the existing decimal serializer in `db.py`

Current behavior keeps all database precision, which creates output like:

```json
"binance": "62067.890000000000000000"
```

Replace the existing `decimal_or_none` helper with a fixed-2-decimal formatter.

Add this near the top of `db.py`:

```python
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional


TWO_DECIMALS = Decimal("0.01")


def decimal_2dp_or_none(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None

    if not isinstance(value, Decimal):
        value = Decimal(str(value))

    return format(value.quantize(TWO_DECIMALS, rounding=ROUND_HALF_UP), "f")
```

Do **not** change the database schema. Keep full precision in PostgreSQL. Only compact the API/download JSON.

---

### 2. Change `build_market_download_payload`

In `db.py`, update the price section from this:

```python
"prices": {
    "binance": decimal_or_none(row["binance_price"]),
    "chainlink": decimal_or_none(row["chainlink_price"]),
},
```

to this:

```python
"prices": {
    "binance": decimal_2dp_or_none(row["binance_price"]),
    "chainlink": decimal_2dp_or_none(row["chainlink_price"]),
},
```

Then update the probability section from this:

```python
item["probabilities"] = {
    "up": {
        "bid": decimal_or_none(row["up_bid"]),
        "ask": decimal_or_none(row["up_ask"]),
        "mid": decimal_or_none(row["up_mid"]),
        "normalized": decimal_or_none(row["up_prob_norm"]),
    },
    "down": {
        "bid": decimal_or_none(row["down_bid"]),
        "ask": decimal_or_none(row["down_ask"]),
        "mid": decimal_or_none(row["down_mid"]),
        "normalized": decimal_or_none(row["down_prob_norm"]),
    },
}
```

to this:

```python
item["probabilities"] = {
    "up": {
        "ask": decimal_2dp_or_none(row["up_ask"]),
    },
    "down": {
        "ask": decimal_2dp_or_none(row["down_ask"]),
    },
}
```

So the download JSON no longer includes:

```text
bid
mid
normalized
```

Only:

```text
up.ask
down.ask
```

---

### 3. Keep `/markets/current/data` and `/markets/{market_id}/data` consistent

Both the data endpoint and download endpoint call `fetch_market_download_payload`, so once `build_market_download_payload` is fixed, both should return the compact shape.

No API route change is needed.

---

## Part 2: fix missing first seconds of probabilities

The current collector discovers/connects **after** the market has already rolled. That is the main reason the first probability rows are missing. The fix is to **preload and pre-connect the next market before the current one ends**.

Polymarket’s Market WebSocket is public, subscribes by token/asset IDs, and supports `custom_feature_enabled: true` for `best_bid_ask`, `new_market`, and `market_resolved` events. The docs also say to send `PING` every 10 seconds on market/user channels. ([Polymarket Documentation][1]) ([Polymarket Documentation][2])

Also add a REST fallback using CLOB `/prices` to prime the first ask values. Polymarket documents that `SELL` returns the best ask for a token and that `/prices` can retrieve prices for multiple token IDs/sides in one request. ([Polymarket Documentation][3]) ([Polymarket Documentation][4])

---

## Add config values

In `config.py`, add:

```python
POLYMARKET_CLOB_BASE_URL: str = "https://clob.polymarket.com"

# Start preparing the next market before the current market ends.
POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS: int = 45

# While trying to discover the next market before the boundary.
POLYMARKET_NEXT_MARKET_RETRY_MS: int = 500

# REST-prime asks near market start in case WebSocket book/best_bid_ask is late.
POLYMARKET_REST_PRIME_SECONDS: int = 15
```

Keep the existing settings:

```python
POLYMARKET_CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_CLOB_PING_SECONDS: int = 10
POLYMARKET_PROBABILITY_STALE_MS: int = 15_000
```

---

## Add REST ask-prime helper

In `polymarket_probability_collector.py`, add this helper.

```python
async def fetch_best_asks_from_clob_prices(
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    base_url = settings.POLYMARKET_CLOB_BASE_URL.rstrip("/")

    request_body = [
        {
            "token_id": current_market.up_token_id,
            "side": "SELL",
        },
        {
            "token_id": current_market.down_token_id,
            "side": "SELL",
        },
    ]

    response = await client.post(
        f"{base_url}/prices",
        json=request_body,
    )
    response.raise_for_status()

    data = json.loads(response.text, parse_float=Decimal)

    def parse_price(token_id: str) -> Optional[Decimal]:
        token_data = data.get(token_id)
        if not isinstance(token_data, Mapping):
            return None

        raw_price = token_data.get("SELL")
        if raw_price is None:
            return None

        try:
            price = raw_price if isinstance(raw_price, Decimal) else Decimal(str(raw_price))
        except (InvalidOperation, ValueError):
            return None

        if not price.is_finite() or price < 0 or price > 1:
            return None

        return price

    return (
        parse_price(current_market.up_token_id),
        parse_price(current_market.down_token_id),
    )
```

Then add:

```python
async def prime_probability_state_from_rest(
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
) -> bool:
    try:
        up_ask, down_ask = await fetch_best_asks_from_clob_prices(
            client,
            settings,
            current_market,
        )
    except Exception as exc:
        LOGGER.debug(
            "polymarket_probability_rest_prime_failed",
            extra={
                "event": "polymarket_probability_rest_prime_failed",
                "market_id": current_market.window.market_id,
                "error": repr(exc),
            },
        )
        return False

    received_ms = current_utc_epoch_ms()
    updated = False

    if up_ask is not None:
        updated = (
            state.update_token(
                current_market.up_token_id,
                bid=None,
                ask=up_ask,
                replace=False,
                provider_event_ms=received_ms,
                received_ms=received_ms,
                event_type="rest_prime_prices",
            )
            or updated
        )

    if down_ask is not None:
        updated = (
            state.update_token(
                current_market.down_token_id,
                bid=None,
                ask=down_ask,
                replace=False,
                provider_event_ms=received_ms,
                received_ms=received_ms,
                event_type="rest_prime_prices",
            )
            or updated
        )

    if updated:
        LOGGER.info(
            "polymarket_probability_rest_prime_updated",
            extra={
                "event": "polymarket_probability_rest_prime_updated",
                "market_id": current_market.window.market_id,
                "up_ask": str(up_ask) if up_ask is not None else None,
                "down_ask": str(down_ask) if down_ask is not None else None,
                "received_ms": received_ms,
            },
        )

    return updated
```

---

## Add short REST prime loop around market start

Add:

```python
async def probability_rest_prime_loop(
    *,
    client: httpx.AsyncClient,
    settings: Settings,
    current_market: CurrentPolymarketMarket,
    state: ProbabilityState,
) -> None:
    # Wait until market start if this session was pre-connected early.
    while current_utc_epoch_ms() < current_market.window.market_start_ms:
        await asyncio.sleep(0.05)

    stop_ms = (
        current_market.window.market_start_ms
        + settings.POLYMARKET_REST_PRIME_SECONDS * 1000
    )

    while current_utc_epoch_ms() < stop_ms:
        await prime_probability_state_from_rest(
            client,
            settings,
            current_market,
            state,
        )
        await asyncio.sleep(seconds_until_next_utc_second())
```

Then modify `collect_current_market` to accept the same `httpx.AsyncClient` used for Gamma/CLOB REST:

```python
async def collect_current_market(
    *,
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    current_market: CurrentPolymarketMarket,
) -> None:
```

Inside `collect_current_market`, after WebSocket subscription and before entering the receive loop, start the REST prime task:

```python
rest_prime_task = asyncio.create_task(
    probability_rest_prime_loop(
        client=client,
        settings=settings,
        current_market=current_market,
        state=state,
    )
)
```

Cancel it in the `finally` block:

```python
rest_prime_task.cancel()
with contextlib.suppress(asyncio.CancelledError):
    await rest_prime_task
```

So the task cleanup section should cancel:

```text
ping_task
sampler_task
rest_prime_task
```

This gives you a fallback source for the first few seconds if the WebSocket `book` or `best_bid_ask` event arrives late.

---

## Modify snapshot logic to require asks, not mids

Since the export only needs ask, the collector should be able to write as soon as both asks are known.

In `build_probability_snapshot`, change this part:

```python
up_mid = midpoint(state.up_bid, state.up_ask)
down_mid = midpoint(state.down_bid, state.down_ask)
if up_mid is None or down_mid is None:
    return None
```

to this:

```python
# For our stored/exported probability snapshots, asks are the required field.
# Bid/mid/normalized can still be stored internally, but they are not required
# for writing a snapshot.
if state.up_ask is None or state.down_ask is None:
    return None

up_mid = midpoint(state.up_bid, state.up_ask)
down_mid = midpoint(state.down_bid, state.down_ask)
```

Keep computing `up_mid`, `down_mid`, and normalized values for the DB columns if you want; the download JSON will no longer expose them.

---

## Preload the next market before boundary

Right now `run_collector` does this:

```python
while True:
    now_ms = current_utc_epoch_ms()
    window = market_for_sample_second(sample_second_ms_for_now(now_ms))
    current_market = await discover_current_polymarket_market(...)
    await collect_current_market(...)
```

That means the next market is not discovered until after the boundary. Replace this with a preloading loop.

Add helpers:

```python
async def sleep_until_ms(target_ms: int) -> None:
    while True:
        now_ms = current_utc_epoch_ms()
        remaining_ms = target_ms - now_ms
        if remaining_ms <= 0:
            return
        await asyncio.sleep(min(remaining_ms / 1000, 1.0))
```

Add:

```python
async def discover_market_with_retries(
    *,
    settings: Settings,
    pool: Any,
    client: httpx.AsyncClient,
    window: MarketWindow,
    deadline_ms: int,
    retry_ms: int,
) -> CurrentPolymarketMarket:
    last_error: Optional[Exception] = None

    while current_utc_epoch_ms() < deadline_ms:
        try:
            return await discover_current_polymarket_market(
                settings,
                pool,
                client,
                window,
            )
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(retry_ms / 1000)

    if last_error is not None:
        raise last_error

    raise GammaDiscoveryError(
        f"could not discover Polymarket market for market_id={window.market_id}"
    )
```

Then rewrite `run_collector` like this conceptually:

```python
async def run_collector(settings: Settings) -> None:
    setup_logging(settings.LOG_LEVEL)

    pool = await create_pool(require_collector_database_url(settings))
    try:
        attempt = 0
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Discover current market first.
            now_ms = current_utc_epoch_ms()
            current_window = market_for_sample_second(sample_second_ms_for_now(now_ms))

            current_market = await discover_current_polymarket_market(
                settings,
                pool,
                client,
                current_window,
            )

            current_task = asyncio.create_task(
                collect_current_market(
                    settings=settings,
                    pool=pool,
                    client=client,
                    current_market=current_market,
                )
            )

            while True:
                try:
                    # Start preparing the next market before this market ends.
                    next_window = market_for_sample_second(
                        current_market.window.market_end_ms
                    )

                    preload_at_ms = (
                        current_market.window.market_end_ms
                        - settings.POLYMARKET_NEXT_MARKET_PRELOAD_SECONDS * 1000
                    )

                    await sleep_until_ms(preload_at_ms)

                    next_market = await discover_market_with_retries(
                        settings=settings,
                        pool=pool,
                        client=client,
                        window=next_window,
                        deadline_ms=current_market.window.market_end_ms - 500,
                        retry_ms=settings.POLYMARKET_NEXT_MARKET_RETRY_MS,
                    )

                    LOGGER.info(
                        "polymarket_next_market_preloaded",
                        extra={
                            "event": "polymarket_next_market_preloaded",
                            "current_market_id": current_market.window.market_id,
                            "next_market_id": next_market.window.market_id,
                            "next_slug": next_market.slug,
                        },
                    )

                    # Start next market collection BEFORE the boundary.
                    # collect_current_market already refuses to write before market_start_ms,
                    # so it is safe to connect early.
                    next_task = asyncio.create_task(
                        collect_current_market(
                            settings=settings,
                            pool=pool,
                            client=client,
                            current_market=next_market,
                        )
                    )

                    # Wait for current market to finish.
                    await current_task

                    # Roll forward without rediscovery delay.
                    current_market = next_market
                    current_task = next_task
                    attempt = 0

                except asyncio.CancelledError:
                    current_task.cancel()
                    raise

                except Exception as exc:
                    attempt += 1
                    delay = reconnect_delay_seconds(attempt)

                    LOGGER.warning(
                        "polymarket_probability_cycle_recovering",
                        extra={
                            "event": "polymarket_probability_cycle_recovering",
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": repr(exc),
                        },
                    )

                    current_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await current_task

                    await asyncio.sleep(delay)

                    # Recover by discovering the current window again.
                    now_ms = current_utc_epoch_ms()
                    current_window = market_for_sample_second(
                        sample_second_ms_for_now(now_ms)
                    )
                    current_market = await discover_current_polymarket_market(
                        settings,
                        pool,
                        client,
                        current_window,
                    )
                    current_task = asyncio.create_task(
                        collect_current_market(
                            settings=settings,
                            pool=pool,
                            client=client,
                            current_market=current_market,
                        )
                    )

    finally:
        await pool.close()
```

Important behavior:

```text
- The next market is discovered before the current market ends.
- The next CLOB WebSocket subscription starts before the new market starts.
- The sampler still writes only when sample_second_ms is inside that market window.
- REST prime fills asks during the first 15 seconds if WebSocket is late.
```

This should remove the first 5–15 second gap in normal conditions.

If Gamma does not expose the next market token IDs until after the market has already started, then the true first seconds cannot be recovered by your collector. The best you can do is retry aggressively before the boundary and use REST prime immediately once token IDs become available.

---

## Do not backfill fake probability rows

Do **not** fill missing first seconds with later asks.

Bad:

```text
t=0 missing, t=1 missing, first ask appears at t=11,
then copy t=11 ask backwards into t=0..10.
```

That would create fake historical data.

Allowed:

```text
Use preconnect + REST prime so real asks are available at t=0.
If unavailable, leave null.
```

---

## Expected new JSON

Before:

```json
"prices": {
  "binance": "62067.890000000000000000",
  "chainlink": "62012.870302750816000000"
},
"probabilities": {
  "up": {
    "bid": "0.55000000",
    "ask": "0.56000000",
    "mid": "0.55500000",
    "normalized": "0.55500000"
  }
}
```

After:

```json
"prices": {
  "binance": "62067.89",
  "chainlink": "62012.87"
},
"probabilities": {
  "up": {
    "ask": "0.56"
  },
  "down": {
    "ask": "0.45"
  }
}
```

Missing values remain `null`:

```json
"probabilities": {
  "up": {
    "ask": null
  },
  "down": {
    "ask": null
  }
}
```

---

## Tests Codex should add

Add unit tests for JSON formatting:

```python
def test_decimal_2dp_or_none():
    assert decimal_2dp_or_none(Decimal("62012.870302750816000000")) == "62012.87"
    assert decimal_2dp_or_none(Decimal("62067.890000000000000000")) == "62067.89"
    assert decimal_2dp_or_none(Decimal("0.55500000")) == "0.56"
    assert decimal_2dp_or_none(Decimal("0.55400000")) == "0.55"
    assert decimal_2dp_or_none(None) is None
```

Add download-shape test:

```python
def test_download_payload_probability_shape_ask_only():
    payload = build_market_download_payload(rows, include_probabilities=True)

    first = payload["series"][0]

    assert set(first["prices"].keys()) == {"binance", "chainlink"}
    assert set(first["probabilities"].keys()) == {"up", "down"}
    assert set(first["probabilities"]["up"].keys()) == {"ask"}
    assert set(first["probabilities"]["down"].keys()) == {"ask"}
```

Add collector behavior tests:

```text
1. next market discovery starts before current market_end_ms.
2. next collect_current_market task starts before next market_start_ms.
3. collect_current_market does not write before market_start_ms.
4. if REST prime provides up_ask/down_ask before t=0, t=0 snapshot is written.
5. if asks are unavailable, snapshot is skipped and not fake-backfilled.
```

---

## Verification commands

After deploy, restart services:

```bash
sudo systemctl daemon-reload
sudo systemctl restart price-collector-polymarket-probabilities
sudo systemctl restart price-api
```

Watch logs around a market boundary:

```bash
sudo journalctl -u price-collector-polymarket-probabilities -f
```

Look for:

```text
polymarket_next_market_preloaded
polymarket_clob_subscribed
polymarket_probability_rest_prime_updated
polymarket_probability_sample_written
```

Then check the newest market:

```bash
curl "http://127.0.0.1:9000/markets/current/data?include_probabilities=true" | jq '.series[0:20]'
```

Expected:

```text
- prices have 2 decimals.
- probabilities include only up.ask and down.ask.
- first probability rows should start much closer to t=0.
```

Final check after a completed market:

```bash
curl -OJ "http://127.0.0.1:9000/markets/current/download?include_probabilities=true"
```

The downloaded rows should look like:

```json
{
  "t": 0,
  "prices": {
    "binance": "62067.89",
    "chainlink": "62013.14"
  },
  "probabilities": {
    "up": {
      "ask": "0.56"
    },
    "down": {
      "ask": "0.45"
    }
  }
}
```



[1]: https://docs.polymarket.com/market-data/websocket/market-channel "Market Channel - Polymarket Documentation"
[2]: https://docs.polymarket.com/market-data/websocket/overview "Overview - Polymarket Documentation"
[3]: https://docs.polymarket.com/api-reference/market-data/get-market-price?utm_source=chatgpt.com "Get market price - Polymarket Documentation"
[4]: https://docs.polymarket.com/api-reference/market-data/get-market-prices-request-body?utm_source=chatgpt.com "Get market prices (request body) - Polymarket Documentation"
