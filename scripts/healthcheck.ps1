# =========================================
# healthcheck.ps1
# One-click health check for Kafka / Connect / Snowflake
# Output: .\health\YYYYMMDD-HHMMSS\
# =========================================

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# 0) timestamp out dir
$ts = (Get-Date).ToString("yyyyMMdd-HHmmss")
$dir = Join-Path $root ("health\"+$ts)
New-Item -ItemType Directory -Force -Path $dir | Out-Null

# 1) Kafka Connect REST
$connectBase = "http://localhost:8083"
$summary = [ordered]@{}
try {
  $rootInfo = Invoke-RestMethod "$connectBase/"
  $summary["connect_rest"] = "OK"
  $rootInfo | ConvertTo-Json -Depth 5 | Out-File (Join-Path $dir "connect_root.json") -Encoding UTF8

  $plugins = Invoke-RestMethod "$connectBase/connector-plugins"
  $plugins | ConvertTo-Json -Depth 5 | Out-File (Join-Path $dir "connect_plugins.json") -Encoding UTF8

  $connectors = @()
  try { $connectors = Invoke-RestMethod "$connectBase/connectors" } catch { $connectors = @() }
  $connectors | ConvertTo-Json -Depth 5 | Out-File (Join-Path $dir "connectors.json") -Encoding UTF8
  $summary["connect_connectors_count"] = $connectors.Count

  if ($connectors -contains "sf-sink-events") {
    $st = Invoke-RestMethod "$connectBase/connectors/sf-sink-events/status"
    $st | ConvertTo-Json -Depth 10 | Out-File (Join-Path $dir "sf-sink-events.status.json") -Encoding UTF8
    $summary["sf_sink_state"] = $st.connector.state
  } else {
    $summary["sf_sink_state"] = "NOT_FOUND"
  }
} catch {
  $summary["connect_rest"] = "DOWN: $($_.Exception.Message)"
}

# 2) Kafka quick checks (topics/lag)
function Run-DockerText($cmd) {
  try {
    $out = (Invoke-Expression $cmd) | Out-String
    return $out
  } catch {
    return "ERROR: $($_.Exception.Message)"
  }
}

$kafkaC = "kafka_flow_data-kafka-1"

# topics list
$topicsCmd = 'docker exec '+$kafkaC+' bash -lc "kafka-topics --bootstrap-server localhost:9092 --list"'
$outTopics = Run-DockerText $topicsCmd
$outTopics | Out-File (Join-Path $dir "kafka_topics.list.txt") -Encoding UTF8

if ($outTopics -match "(?m)^events$") {
  $summary["kafka_topics_contains_events"] = "YES"
} else {
  $summary["kafka_topics_contains_events"] = "NO"
}

# events describe
$descCmd = 'docker exec '+$kafkaC+' bash -lc "kafka-topics --describe --topic events --bootstrap-server localhost:9092"'
$outDesc = Run-DockerText $descCmd
$outDesc | Out-File (Join-Path $dir "kafka_events.describe.txt") -Encoding UTF8

# consumer group lag
$lagCmd = 'docker exec '+$kafkaC+' bash -lc "kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-sf-sink-events --describe || true"'
$outLag = Run-DockerText $lagCmd
$outLag | Out-File (Join-Path $dir "kafka_consumer_group_connect-sf-sink-events.txt") -Encoding UTF8

if ($outLag -match "has no active members") {
  $summary["kafka_consumer_group_notice"] = "NO_ACTIVE_MEMBERS"
} else {
  $summary["kafka_consumer_group_notice"] = "OK_OR_UNKNOWN"
}

# 3) Snowflake checks
$snowsql = Get-Command snowsql -ErrorAction SilentlyContinue
if ($null -ne $snowsql) {
  $summary["snowsql_found"] = "YES"
  $sql1 = "SELECT COUNT(*) AS n, MIN(event_time) AS min_t, MAX(event_time) AS max_t FROM DEMO_DW.MART_MON.F_EVENTS_PARSED WHERE event_time >= DATEADD(minute,-15,CURRENT_TIMESTAMP());"
  try {
    $tmp = Join-Path $dir "snowflake_result.txt"
    & snowsql -q $sql1 | Out-File $tmp -Encoding UTF8
    $summary["snowflake_query"] = "OK"
  } catch {
    $summary["snowflake_query"] = "FAILED: $($_.Exception.Message)"
  }
} else {
  $summary["snowsql_found"] = "NO"
  $sqlFile = Join-Path $dir "snowflake_check.sql"
  @"
-- Open in Snowsight and run:
USE ROLE ACCOUNTADMIN; ALTER SESSION SET TIMEZONE='UTC';
SELECT COUNT(*) AS n, MIN(event_time) AS min_t, MAX(event_time) AS max_t
FROM DEMO_DW.MART_MON.F_EVENTS_PARSED
WHERE event_time >= DATEADD(minute,-15,CURRENT_TIMESTAMP());
"@ | Out-File $sqlFile -Encoding UTF8
}

# 4) write summary.json and console
$summary | ConvertTo-Json -Depth 5 | Out-File (Join-Path $dir "summary.json") -Encoding UTF8

Write-Host ""
Write-Host "=== HEALTH CHECK SUMMARY ==="
$summary.GetEnumerator() | ForEach-Object { Write-Host ("{0} : {1}" -f $_.Key, $_.Value) }
Write-Host ("Output folder: {0}" -f $dir)
