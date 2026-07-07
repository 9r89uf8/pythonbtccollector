# Operations

Use this after the droplet is already deployed and working.

## Check Services

On the droplet:

```bash
systemctl status price-collector --no-pager
systemctl status price-api --no-pager
```

Follow logs:

```bash
journalctl -u price-collector -f
journalctl -u price-api -f
```

Check the local API from inside the droplet:

```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl http://127.0.0.1:9000/markets/latest
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
```

Keep the SSH tunnel terminal open while using the API locally.

## Deploy Code Updates

After pushing changes to GitHub, update the droplet:

```bash
cd /opt/price-collector
sudo -u pricecollector git pull --ff-only
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
systemctl restart price-collector price-api
```

Verify after restart:

```bash
systemctl status price-collector --no-pager
systemctl status price-api --no-pager
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/markets/latest
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

## Confirm Local-Only Binding

On the droplet:

```bash
ss -ltnp | grep ':9000'
ss -ltnp | grep ':5432'
```

Acceptable:

```text
127.0.0.1:9000
127.0.0.1:5432
```

Not acceptable:

```text
0.0.0.0:9000
0.0.0.0:5432
```
