# Operations

Use this after the droplet is already deployed and working.

## Check Services

On the droplet:

```bash
systemctl status price-collector --no-pager
systemctl status price-collector-polymarket-chainlink --no-pager
systemctl status price-collector-binance-futures --no-pager
systemctl status redis-server --no-pager
systemctl status price-api --no-pager
```

Follow logs:

```bash
journalctl -u price-collector -f
journalctl -u price-collector-polymarket-chainlink -f
journalctl -u price-collector-binance-futures -f
journalctl -u price-api -f
```

Check the local API from inside the droplet:

```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
curl http://127.0.0.1:9000/markets/latest
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

## Connect From Your Local Machine

The API is intentionally bound only to `127.0.0.1` on the droplet. Use an SSH tunnel from your local machine:

Copy the example env file and put your droplet IP in the local ignored file:

```bash
cp droplet.env.example droplet.env
nano droplet.env
```

Load it:

```bash
set -a
. ./droplet.env
set +a
```

Open the tunnel:

```bash
ssh -N -L "${LOCAL_API_PORT}:127.0.0.1:9000" "${DROPLET_USER}@${DROPLET_IP}"
```

Then, from your local machine:

```bash
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/latest"
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/sources"
curl "http://127.0.0.1:${LOCAL_API_PORT}/markets/current/live"
```

Keep the SSH tunnel terminal open while using the API locally.

## Deploy Code Updates

If this is the first deploy after adding the live Redis cache, install and bind Redis locally before restarting the app services:

```bash
sudo apt update
sudo apt install -y redis-server
sudo sed -i 's/^bind .*/bind 127.0.0.1/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
```

After pushing code-only changes to GitHub, update the droplet:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
systemctl restart redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-api
```

If the update adds database columns, seed rows, indexes, or new systemd unit files, use this fuller sequence instead. Apply the schema before restarting services so new code does not start before PostgreSQL has the expected tables, columns, and seed data:

```bash
cd /opt/price-collector

sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt

sudo -u postgres psql -d price_collector -f /opt/price-collector/schema.sql

sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service
sudo systemctl daemon-reload
sudo systemctl enable redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-api

sudo systemctl restart redis-server price-collector price-collector-polymarket-chainlink price-collector-binance-futures price-api
```

Verify after restart:

```bash
systemctl status price-collector --no-pager
systemctl status price-collector-polymarket-chainlink --no-pager
systemctl status price-collector-binance-futures --no-pager
systemctl status redis-server --no-pager
systemctl status price-api --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/markets/latest
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```

## Redis Spot Checks

Redis is the live-card cache only. PostgreSQL remains the historical source.

```bash
redis-cli -h 127.0.0.1 MGET btc:live:binance_spot btc:live:chainlink btc:live:futures
```

Each populated key should look like:

```json
{"value":"62067.89","source_timestamp_ms":123,"received_ms":456}
```

## Database Spot Checks

On the droplet:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    count(*) AS rows,
    min(sample_second_at) AS first_sample,
    max(sample_second_at) AS latest_sample
FROM price_samples;
"
```

Latest market counts:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT
    mw.market_id,
    mw.market_start_at,
    mw.market_end_at,
    count(ps.*) AS sample_count
FROM market_windows mw
JOIN price_samples ps ON ps.market_id = mw.market_id
GROUP BY mw.market_id, mw.market_start_at, mw.market_end_at
ORDER BY mw.market_id DESC
LIMIT 10;
"
```

Futures flow and top-of-book row counts:

```bash
sudo -u postgres psql -d price_collector -c "
SELECT 'binance_flow_1s' AS table_name, count(*) AS rows, max(sample_second_at) AS latest_sample
FROM binance_flow_1s
UNION ALL
SELECT 'binance_book_1s' AS table_name, count(*) AS rows, max(sample_second_at) AS latest_sample
FROM binance_book_1s;
"
```

## Confirm Local-Only Binding

On the droplet:

```bash
ss -ltnp | grep ':9000'
ss -ltnp | grep ':5432'
ss -ltnp | grep ':6379'
```

Acceptable:

```text
127.0.0.1:9000
127.0.0.1:5432
127.0.0.1:6379
```

Not acceptable:

```text
0.0.0.0:9000
0.0.0.0:5432
0.0.0.0:6379
```
