# AGENTS.md

Guidance for future work in this repository.

## Project Goal

Build a production-ready Python price collector for a single-user Ubuntu 24.04 DigitalOcean droplet.

The deployed system is:

- Local PostgreSQL database named `price_collector`
- Python Binance Spot collector managed by systemd
- Tiny read-only FastAPI API managed by systemd
- API bound only to `127.0.0.1:9000`
- No public PostgreSQL exposure
- No public API exposure
- No Docker
- No TypeScript
- No dashboard on the droplet

## Implementation Rules

- Work in reviewable checkpoints. Do not implement the entire system in one session unless the user explicitly asks.
- Follow `plan.md` as the primary blueprint.
- Keep the project Python-only.
- Use the requested package layout under `price_collector/`.
- Use `Decimal` for prices. Never convert prices to `float`.
- Use UTC epoch milliseconds for sampling and market windows.
- Keep Binance stream symbols lowercase in the stream name: `btcusdt@ticker`.
- Treat the API as read-only.
- Keep the API host fixed to `127.0.0.1` in systemd.
- Do not add Docker, Compose, frontend code, or dashboard assets.

## Collector Rules

- Connect to `wss://stream.binance.com:9443/ws/btcusdt@ticker`.
- Parse Binance ticker payload field `c` as the last price.
- Parse payload field `E` as provider event time in milliseconds.
- Keep the latest received price in memory.
- Write at most one sample per UTC second.
- Skip writes when the latest price is older than `STALE_PRICE_MS`, default `10000`.
- Reconnect automatically on websocket errors.
- Use exponential backoff with jitter, capped at 60 seconds.
- Proactively reconnect before 24 hours, using about 23h 50m.

## Database Rules

- Store prices as PostgreSQL `NUMERIC(38,18)`.
- The `price_samples` primary key must be `(instrument_id, sample_second_ms)`.
- Duplicate inserts in the same second should update the existing row or otherwise not create a duplicate.
- Seed:
  - `provider_code`: `binance_spot`
  - `symbol`: `BTCUSDT`
  - `base_asset`: `BTC`
  - `quote_asset`: `USDT`
  - `stream_name`: `btcusdt@ticker`

## Market Window Rule

For every saved sample:

```python
market_start_ms = (sample_second_ms // 300_000) * 300_000
market_end_ms = market_start_ms + 300_000
market_id = market_start_ms // 300_000
```

Boundary behavior:

- `[4:05:00.000, 4:10:00.000)`
- `[4:10:00.000, 4:15:00.000)`
- Exactly `4:10:00.000` belongs to the new market.

## Testing Expectations

Add or update tests for each checkpoint:

- Market boundary behavior
- Binance ticker parsing
- Duplicate same-second insert behavior
- API latest market response
- API bind host documented as `127.0.0.1` only

Run relevant tests before handing off each checkpoint when possible.

