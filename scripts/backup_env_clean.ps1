# ===============================================
# backup_env_clean.ps1
# Purpose: Snapshot Kafka / Connect / Snowflake configs and generate DDL export SQLs
# ===============================================

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# 1) Create timestamped backup directory
$stamp = (Get-Date).ToString('yyyyMMdd-HHmmss')
$bk = Join-Path $root "backup\$stamp"
New-Item -ItemType Directory -Force -Path $bk | Out-Null
Write-Host "Backup folder: $bk"

# 2) Static files snapshot
$staticFiles = @(
  'docker-compose.yml',
  'docker-compose.override.yml',
  'Dockerfile.connect',
  'sf-sink-events.config.json',
  'sf-sink.json',
  'sf-sink-new.json',
  'topic-events.describe.txt'
) | Where-Object { Test-Path $_ }

foreach ($f in $staticFiles) {
  Copy-Item $f -Destination $bk -Force
}
Write-Host "[OK] Static files snapshot done."

# 3) Connect plugins and connector configs
$connectBase = "http://localhost:8083"
try {
  $plugins = Invoke-RestMethod "$connectBase/connector-plugins"
  $plugins | ConvertTo-Json -Depth 5 | Out-File (Join-Path $bk "connect-plugins.json") -Encoding UTF8

  $connectors = @()
  try { $connectors = Invoke-RestMethod "$connectBase/connectors" } catch { $connectors = @() }
  $connectors | ConvertTo-Json -Depth 5 | Out-File (Join-Path $bk "connectors.json") -Encoding UTF8

  foreach ($c in $connectors) {
    try {
      $cfg = Invoke-RestMethod "$connectBase/connectors/$c/config"
      $cfg | ConvertTo-Json -Depth 10 | Out-File (Join-Path $bk "connector_$($c)_config.json") -Encoding UTF8
    } catch {
      Write-Warning "Cannot get connector config for $c : $($_.Exception.Message)"
    }
  }
  Write-Host "[OK] Connect plugin & connector configs exported."
}
catch {
  Write-Warning "Connect API not available: $($_.Exception.Message)"
}

# 4) Kafka topics metadata
$kafkaC = "kafka_flow_data-kafka-1"
$topicListFile = Join-Path $bk "topics.list.txt"
try {
  docker exec $kafkaC bash -lc "kafka-topics --bootstrap-server localhost:9092 --list" | Out-File $topicListFile -Encoding UTF8
  $topics = Get-Content $topicListFile | Where-Object { $_ -ne '' }

  foreach ($t in $topics) {
    $safe = ($t -replace '[^a-zA-Z0-9\-_\.]', '_')
    $descFile = Join-Path $bk "topic-$safe.describe.txt"
    docker exec $kafkaC bash -lc "kafka-topics --describe --topic '$t' --bootstrap-server localhost:9092" | Out-File $descFile -Encoding UTF8
  }

  $ofs = Join-Path $bk "consumer_offsets.snapshot.txt"
  docker exec $kafkaC bash -lc "kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe" | Out-File $ofs -Encoding UTF8

  Write-Host "[OK] Kafka topics & offsets exported."
}
catch {
  Write-Warning "Kafka container command failed: $($_.Exception.Message)"
}

# 5) Generate Snowflake DDL export SQLs
$rawExportSql = @'
SELECT
  '-- ========= '||table_catalog||'.'||table_schema||'.'||table_name||' =========' || CHR(10) ||
  'SELECT GET_DDL(''TABLE'','''||table_catalog||'.'||table_schema||'.'||table_name||''') AS ddl;'
AS script_block
FROM DEMO_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'RAW'
ORDER BY 1;
'@
$rawExportSql | Out-File (Join-Path $bk 'export_RAW_generate.sql') -Encoding UTF8

$martExportSql = @'
SELECT
  '-- ========= '||table_catalog||'.'||table_schema||'.'||table_name||' =========' || CHR(10) ||
  'SELECT GET_DDL(''TABLE'','''||table_catalog||'.'||table_schema||'.'||table_name||''') AS ddl;'
AS script_block
FROM DEMO_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'MART'
ORDER BY 1;
'@
$martExportSql | Out-File (Join-Path $bk 'export_MART_generate.sql') -Encoding UTF8

$monExportSql = @'
SELECT
  '-- ========= '||table_catalog||'.'||table_schema||'.'||table_name||' =========' || CHR(10) ||
  'SELECT GET_DDL(''TABLE'','''||table_catalog||'.'||table_schema||'.'||table_name||''') AS ddl;'
AS script_block
FROM DEMO_DW.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'MART_MON'
ORDER BY 1;
'@
$monExportSql | Out-File (Join-Path $bk 'export_MART_MON_generate.sql') -Encoding UTF8

Write-Host "[OK] Generated 3 Snowflake DDL export SQL templates."

# 6) Summary (ASCII only)
Write-Host ""
Write-Host "=== Backup summary ==="
Write-Host ("Backup folder: {0}" -f $bk)

if (Test-Path (Join-Path $bk 'connect-plugins.json')) {
  Write-Host " - Connect plugins/config : OK"
} else {
  Write-Host " - Connect plugins/config : MISSING"
}

if (Test-Path $topicListFile) {
  Write-Host " - Kafka topics list/describe : OK"
} else {
  Write-Host " - Kafka topics list/describe : MISSING"
}

$haveRaw  = Test-Path (Join-Path $bk 'export_RAW_generate.sql')
$haveMart = Test-Path (Join-Path $bk 'export_MART_generate.sql')
$haveMon  = Test-Path (Join-Path $bk 'export_MART_MON_generate.sql')

if ($haveRaw -and $haveMart) {
  Write-Host " - Snowflake DDL export scripts generated:"
  Write-Host "   - export_RAW_generate.sql"
  Write-Host "   - export_MART_generate.sql"
  if ($haveMon) { Write-Host "   - export_MART_MON_generate.sql" }
} else {
  Write-Host " - Snowflake DDL export scripts : MISSING"
}

Write-Host "======================"
