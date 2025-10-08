# Kafka 鈫?Snowflake Demo

## Quick Start
1. `cp .env.example .env` 骞跺～鍐欎笂闈㈢殑璐﹀彿锛堢閽ュ彟瑙佷笅鏂囷級銆?
2. Snowflake 涓緷娆¤繍琛?`snowflake/000*`銆乣010*`銆乣020*`銆乣030*`銆?
3. `docker compose -f docker/docker-compose.yml up -d --build`
4. `pwsh connector/create_connector.ps1 -PrivateKeyPath .\secrets\private_key.p8`锛堥殢鍚?`Invoke-RestMethod http://localhost:8083/connectors` 鍙湅鍒?connector锛?
5. `python scripts/producer_weather.py` 鍙戦€佹牱渚嬫暟鎹€?
6. `pwsh scripts/healthcheck.ps1` 鏌ョ湅鍋ュ悍妫€鏌ヨ緭鍑恒€?

> 娉ㄦ剰锛氫粨搴撲笉鍖呭惈绉侀挜鏂囦欢锛屾斁鍦?`secrets/private_key.p8`锛屼笉瑕佹彁浜ゅ埌 Git銆?
