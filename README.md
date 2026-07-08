Price Collector
Production-oriented BTC price collection for a single-user Ubuntu 24.04 DigitalOcean droplet.
Architecture
```text
DigitalOcean droplet
├── systemd
│   ├── price-collector.service
│   ├── price-collector-polymarket-chainlink.service
│   ├── price-collector-binance-futures.service
│   └── price-api.service
├── Python app cloned from GitHub into /opt/price-collector
├── env files in /etc/price-collector
├── local Redis live cache bound to 127.0.0.1:6379
└── local PostgreSQL database price_collector
```
The Binance collector connects to Binance Spot WebSocket stream `btcusdt@ticker`, writes the latest event to Redis immediately, keeps the latest price in memory, and writes at most one PostgreSQL sample per UTC second. The Polymarket collector connects to Polymarket RTDS topic `crypto_prices_chainlink` with filter `{"symbol":"btc/usd"}` and writes Chainlink BTC/USD samples by the source payload timestamp. The Binance futures collector polls REST and writes its latest price to Redis before historical storage. The API is read-only, `/markets/current/live` reads Redis, and the API must bind only to `127.0.0.1:9000`.
Do not run Docker. Do not run a dashboard on the droplet. Do not expose PostgreSQL or the API publicly.
Droplet Assumptions
Ubuntu 24.04 LTS
DigitalOcean Singapore region
1 vCPU / 2 GB RAM is enough for this initial collector
SSH key access to the droplet
No public API port
No public PostgreSQL port
Install Packages
```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip postgresql postgresql-contrib redis-server git openssh-client ufw
```
Firewall
```bash
sudo ufw allow OpenSSH
sudo ufw enable
sudo ufw status
```
Do not run:
```bash
sudo ufw allow 9000
sudo ufw allow 5432
sudo ufw allow 6379
```
Service User
```bash
sudo useradd --system --home /var/lib/price-collector --shell /usr/sbin/nologin pricecollector
sudo mkdir -p /var/lib/price-collector
sudo chown -R pricecollector:pricecollector /var/lib/price-collector
```
GitHub Access
The droplet should install the collector by cloning the GitHub repository.
For a public repository, use an HTTPS URL:
```bash
export PRICE_COLLECTOR_REPO="https://github.com/YOUR_USERNAME/YOUR_REPO.git"
export PRICE_COLLECTOR_BRANCH="main"
```
For a private repository, prefer a read-only GitHub deploy key and use an SSH URL:
```bash
export PRICE_COLLECTOR_REPO="git@github.com:YOUR_USERNAME/YOUR_REPO.git"
export PRICE_COLLECTOR_BRANCH="main"
```
Optional: private repository deploy key
Skip this section if the GitHub repository is public.
Create an SSH key owned by the `pricecollector` user:
```bash
sudo -u pricecollector mkdir -p /var/lib/price-collector/.ssh
sudo chmod 700 /var/lib/price-collector/.ssh
sudo -u pricecollector ssh-keygen -t ed25519 -C "price-collector-droplet" -f /var/lib/price-collector/.ssh/github_deploy_key -N ""
```
Print the public key:
```bash
sudo cat /var/lib/price-collector/.ssh/github_deploy_key.pub
```
Add that public key to the GitHub repository as a read-only deploy key.
Create the SSH config:
```bash
sudo -u pricecollector tee /var/lib/price-collector/.ssh/config >/dev/null <<'EOF_SSH_CONFIG'
Host github.com
    HostName github.com
    User git
    IdentityFile /var/lib/price-collector/.ssh/github_deploy_key
    IdentitiesOnly yes
EOF_SSH_CONFIG

sudo chmod 600 /var/lib/price-collector/.ssh/config
sudo chown -R pricecollector:pricecollector /var/lib/price-collector/.ssh
```
Add GitHub to known hosts:
```bash
sudo -u pricecollector sh -c 'ssh-keyscan github.com >> /var/lib/price-collector/.ssh/known_hosts'
sudo chmod 600 /var/lib/price-collector/.ssh/known_hosts
```
Test access:
```bash
sudo -u pricecollector ssh -T git@github.com
```
GitHub may print a message saying shell access is not provided. That is normal as long as authentication succeeds.
Clone App From GitHub
Create the application directory:
```bash
sudo mkdir -p /opt/price-collector
sudo chown -R pricecollector:pricecollector /opt/price-collector
```
Clone the repository into `/opt/price-collector`:
```bash
sudo -u pricecollector git clone --branch "$PRICE_COLLECTOR_BRANCH" "$PRICE_COLLECTOR_REPO" /opt/price-collector
```
If `/opt/price-collector` already exists and is not empty, either remove the incomplete directory or clone into a temporary path and move it into place.
Install Python dependencies:
```bash
cd /opt/price-collector
sudo -u pricecollector python3 -m venv .venv
sudo -u pricecollector .venv/bin/pip install --upgrade pip
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
```
Run the local unit suite if desired:
```bash
sudo -u pricecollector .venv/bin/python -m pytest
```
The unit tests do not require live Binance or live PostgreSQL.
PostgreSQL Setup
Open psql:
```bash
sudo -u postgres psql
```
Create the database and roles:
```sql
CREATE DATABASE price_collector;

CREATE USER price_writer WITH PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';
CREATE USER price_reader WITH PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

GRANT CONNECT ON DATABASE price_collector TO price_writer;
GRANT CONNECT ON DATABASE price_collector TO price_reader;
```
Exit psql:
```sql
\q
```
Load the schema from the cloned repository:
```bash
sudo -u postgres psql -d price_collector -f /opt/price-collector/schema.sql
```

Redis Setup
Keep Redis private to the droplet:
```bash
sudo sed -i 's/^bind .*/bind 127.0.0.1/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode .*/protected-mode yes/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
```
Verify Redis is listening only on loopback:
```bash
sudo ss -ltnp | grep ':6379'
```
Acceptable:
```text
127.0.0.1:6379
```
Not acceptable:
```text
0.0.0.0:6379
```
Apply grants:
```bash
sudo -u postgres psql -d price_collector
```
```sql
GRANT USAGE ON SCHEMA public TO price_writer;
GRANT USAGE ON SCHEMA public TO price_reader;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO price_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO price_writer;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO price_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE ON TABLES TO price_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO price_reader;
```
Exit psql:
```sql
\q
```
Environment Files
Use separate env files. The API env file must not contain the writer DB password.
```bash
sudo mkdir -p /etc/price-collector

sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/collector.env.example /etc/price-collector/collector.env
sudo install -o root -g pricecollector -m 640 /opt/price-collector/deployment/api.env.example /etc/price-collector/api.env
```
Edit the passwords:
```bash
sudo nano /etc/price-collector/collector.env
sudo nano /etc/price-collector/api.env
```
`collector.env` contains writer credentials:
```text
DATABASE_URL=postgresql://price_writer:REPLACE_ME@127.0.0.1:5432/price_collector
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```
It can also override the Polymarket Chainlink RTDS defaults:
```text
POLYMARKET_RTDS_WS_URL=wss://ws-live-data.polymarket.com
POLYMARKET_CHAINLINK_PROVIDER_CODE=polymarket_chainlink_rtds
POLYMARKET_CHAINLINK_SYMBOL=BTCUSD
POLYMARKET_CHAINLINK_RTD_SYMBOL=btc/usd
POLYMARKET_CHAINLINK_TOPIC=crypto_prices_chainlink
```
`api.env` contains reader credentials only:
```text
READ_DATABASE_URL=postgresql://price_reader:REPLACE_ME@127.0.0.1:5432/price_collector
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
```
Do not commit real `.env`, `collector.env`, or `api.env` files to GitHub. Commit only the example env files.
systemd
Install service files from the cloned repository:
```bash
sudo cp /opt/price-collector/deployment/price-collector.service /etc/systemd/system/price-collector.service
sudo cp /opt/price-collector/deployment/price-collector-polymarket-chainlink.service /etc/systemd/system/price-collector-polymarket-chainlink.service
sudo cp /opt/price-collector/deployment/price-collector-binance-futures.service /etc/systemd/system/price-collector-binance-futures.service
sudo cp /opt/price-collector/deployment/price-api.service /etc/systemd/system/price-api.service
sudo systemctl daemon-reload
sudo systemctl enable price-collector
sudo systemctl enable price-collector-polymarket-chainlink
sudo systemctl enable price-collector-binance-futures
sudo systemctl enable price-api
sudo systemctl start price-collector
sudo systemctl start price-collector-polymarket-chainlink
sudo systemctl start price-collector-binance-futures
sudo systemctl start price-api
```
The API service is intentionally local-only:
```text
ExecStart=/opt/price-collector/.venv/bin/uvicorn price_collector.api:app --host 127.0.0.1 --port 9000 --workers 1
```
Service Verification
```bash
sudo systemctl status price-collector
sudo systemctl status price-collector-polymarket-chainlink
sudo systemctl status price-collector-binance-futures
sudo systemctl status price-api
```
Logs:
```bash
sudo journalctl -u price-collector -f
sudo journalctl -u price-collector-polymarket-chainlink -f
sudo journalctl -u price-collector-binance-futures -f
sudo journalctl -u price-api -f
```
API checks from inside the droplet:
```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/prices/latest
curl "http://127.0.0.1:9000/prices/latest?provider=polymarket_chainlink_rtds&symbol=BTCUSD"
curl http://127.0.0.1:9000/markets/latest
curl http://127.0.0.1:9000/markets/current/sources
curl http://127.0.0.1:9000/markets/current/live
```
DB Verification
```bash
sudo -u postgres psql -d price_collector
```
```sql
SELECT
    count(*) AS rows,
    min(sample_second_at) AS first_sample,
    max(sample_second_at) AS latest_sample
FROM price_samples;
```
```sql
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
```
Exit psql:
```sql
\q
```
Confirm Local-Only Binding
```bash
sudo ss -ltnp | grep ':9000'
sudo ss -ltnp | grep ':5432'
sudo ss -ltnp | grep ':6379'
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
SSH Tunnel
From your local machine:
```bash
ssh -N -L 9000:127.0.0.1:9000 root@YOUR_DROPLET_IP
```
Then from your local machine:
```bash
curl http://127.0.0.1:9000/markets/latest
```
Deploy Updates From GitHub
When you push a new version to GitHub, update the droplet by pulling the latest code.
```bash
cd /opt/price-collector
sudo -u pricecollector git fetch --all --prune
sudo -u pricecollector git checkout "$PRICE_COLLECTOR_BRANCH"
sudo -u pricecollector git pull --ff-only origin "$PRICE_COLLECTOR_BRANCH"
```
Update Python dependencies:
```bash
sudo -u pricecollector .venv/bin/pip install -r requirements.txt
```
If `schema.sql` changed, review it before applying it. For this version, the schema file is safe for initial setup, but future schema changes should be handled with explicit migrations.
Restart services:
```bash
sudo systemctl restart price-collector
sudo systemctl restart price-collector-polymarket-chainlink
sudo systemctl restart price-collector-binance-futures
sudo systemctl restart price-api
```
Verify after update:
```bash
sudo systemctl status price-collector
sudo systemctl status price-collector-polymarket-chainlink
sudo systemctl status price-collector-binance-futures
sudo systemctl status price-api
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/markets/current/live
```
Maintenance Notes
One provider at one sample per second is 86,400 rows/day.
More providers multiply that rate.
Monitor disk with `df -h`.
Monitor PostgreSQL size with:
```sql
SELECT pg_size_pretty(pg_database_size('price_collector'));
```
No automatic pruning is included yet.# pythonbtccollector
