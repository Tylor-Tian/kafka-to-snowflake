# Kafka → Snowflake Demo

[![Docker](https://img.shields.io/badge/Docker-ready-blue)]()
[![Connector](https://img.shields.io/badge/Snowflake%20Kafka%20Connector-3.3.0-brightgreen)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A minimal, production-oriented demo that streams events from **Kafka** to **Snowflake** via Kafka Connect (Snowflake Sink), with:
- RAW landing table (`EVENTS_RAW_B`)
- Robust JSON parsing view (`V_EVENTS_PARSED_RAW`) and fact table (`F_EVENTS_PARSED`)
- Optional **Stream + Task** for 5-minute incremental loads
- Healthcheck & backup scripts

---

## Quick Start

### 0) Prereqs
- Docker Desktop
- Python 3.x (for the sample producer)
- Snowflake account + key pair (private key stays local, **never** checked into git)

### 1) Clone & prepare env
```bash
git clone https://github.com/Tylor-Tian/kafka-to-snowflake.git
cd kafka-to-snowflake
cp .env.example .env   # fill your Snowflake account etc.
2) Create Snowflake objects
In Snowsight, run in order:

snowflake/000_prereq_roles_warehouses.sql

snowflake/010_raw_stage_tables.sql

snowflake/020_mart_views.sql

snowflake/030_mon_schema_tasks.sql

3) Bring up Kafka/ZooKeeper/Connect

docker compose -f docker/docker-compose.yml up -d --build
4) Create the Snowflake Sink connector
Copy your private key to secrets/private_key.p8 (not tracked), then:


pwsh connector/create_connector.ps1 -PrivateKeyPath .\secrets\private_key.p8
# verify:
Invoke-RestMethod http://localhost:8083/connectors
5) Send sample data

python scripts/producer_weather.py
6) Health check

pwsh scripts/healthcheck.ps1
Project Structure

docker/        # docker-compose & connect Dockerfile
connector/     # connector template & creation script (no secrets)
scripts/       # sample producer, healthcheck, backup
snowflake/     # SQL: roles/wh, RAW tables, MART views, MON tasks
docs/          # (optional) architecture, ops
Notes
Secrets are local only (secrets/private_key.p8) and ignored by git.

The parsing view handles cases where the Kafka value is a JSON string with extra escape or a trailing literal `n.

License
MIT
