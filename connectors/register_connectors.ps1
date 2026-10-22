# ═══════════════════════════════════════════════════════════════════════
# register_connectors.ps1
# Idempotent registration of:
#   1. Debezium PostgreSQL SOURCE connector (JSON CDC topics)
#   2. JDBC SINK connectors (analytics PostgreSQL: metrics + alerts)
#   3. (optional -RegisterSchemas) pre-seeds the Avro subjects so the
#      schema-evolution demo (tests/03) is deterministic.
#
# Idempotency: an existing connector's config is PUT (replace);
#   a missing connector is POSTed. Re-running always converges to the
#   intended state and reports RUNNING/UNASSIGNED etc.
#
# Usage:
#   .\connectors\register_connectors.ps1
#   .\connectors\register_connectors.ps1 -RegisterSchemas
#   .\connectors\register_connectors.ps1 -SkipSinks        # CDC only
#   .\connectors\register_connectors.ps1 -StatusOnly
# ═══════════════════════════════════════════════════════════════════════
[CmdletBinding()]
param(
    [switch]$RegisterSchemas,
    [switch]$SkipSinks,
    [switch]$StatusOnly
)

$ErrorActionPreference = 'Stop'

# ── locate repo root + load .env ───────────────────────────────────────
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not $repoRoot) { $repoRoot = (Get-Location).Path }
$envFile = Join-Path $repoRoot '.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=\s]+)\s*=\s*(.*)\s*$') {
            $k = $matches[1]; $v = $matches[2]
            if (-not (Get-Item "env:$k" -ErrorAction SilentlyContinue)) { Set-Item "env:$k" $v }
        }
    }
}

# defaults (must match .env.example)
$KafkaConnectUrl  = $env:KAFKA_CONNECT_URL;   if (-not $KafkaConnectUrl)  { $KafkaConnectUrl = 'http://localhost:8083' }
$SchemaRegistryUrl = $env:SCHEMA_REGISTRY_URL; if (-not $SchemaRegistryUrl) { $SchemaRegistryUrl = 'http://localhost:8081' }

$SrcUser   = $env:POSTGRES_USER;              if (-not $SrcUser)   { $SrcUser   = 'postgres' }
$SrcPass   = $env:POSTGRES_PASSWORD;          if (-not $SrcPass)   { $SrcPass   = 'postgres' }
$SrcDb     = $env:POSTGRES_DB;                if (-not $SrcDb)     { $SrcDb     = 'recruitment' }
$DbHost    = $env:DEBEZIUM_DB_HOST;           if (-not $DbHost)    { $DbHost    = 'postgres' }
$DbPort    = '5432'                           # from inside the Connect container
$DbPrefix  = $env:DEBEZIUM_TOPIC_PREFIX;      if (-not $DbPrefix)  { $DbPrefix  = 'talent' }
$DbSlot    = $env:DEBEZIUM_SLOT_NAME;         if (-not $DbSlot)    { $DbSlot    = 'talent_cdc_slot' }
$CxName    = $env:DEBEZIUM_CONNECTOR_NAME;    if (-not $CxName)    { $CxName    = 'candidate-job-matching-cdc' }

$AnDbName  = $env:ANALYTICS_POSTGRES_DB;      if (-not $AnDbName)  { $AnDbName  = 'analytics' }
$AnDbUser  = $env:ANALYTICS_POSTGRES_USER;    if (-not $AnDbUser)  { $AnDbUser  = 'postgres' }
$AnDbPass  = $env:ANALYTICS_POSTGRES_PASSWORD;if (-not $AnDbPass)  { $AnDbPass  = 'postgres' }

function Invoke-Api {
    param([string]$Url, [string]$Method = 'GET', $Body = $null)
    $params = @{ Uri = $Url; Method = $Method; ErrorAction = 'Stop' }
    if ($null -ne $Body) {
        $params.Body = ($Body | ConvertTo-Json -Depth 64 -Compress)
        $params.ContentType = 'application/json'
    }
    try { return Invoke-RestMethod @params }
    catch {
        $resp = $_.Exception.Response
        $detail = ''
        if ($resp -and $resp.GetResponseStream) {
            $reader  = New-Object System.IO.StreamReader($resp.GetResponseStream())
            $detail  = $reader.ReadToEnd()
        }
        throw "HTTP $Method $Url failed: $($_.Exception.Message)`n$detail"
    }
}

function Wait-ConnectReady {
    Write-Host "Waiting for Kafka Connect $($KafkaConnectUrl) ..."
    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        try {
            $info = Invoke-Api "$KafkaConnectUrl/" -Method GET
            Write-Host "  Connect is up (version $($info.version))."
            return
        } catch { Start-Sleep -Seconds 3 }
    }
    throw "Kafka Connect not reachable after 120s. Is the stack running? (docker compose up -d)"
}

function Convert-ScalarStrings {
    # Kafka Connect REST API expects booleans/numbers as JSON booleans/numbers
    # in the config map. Our connector files store everything as strings
    # (typical hand-written configs); Connect accepts string VALUES fine -
    # only the classic /connectors POST validation may complain, and Debezium
    # configs are string-key/string-value maps anyway. No conversion needed.
}

# ── connector registration (idempotent) ───────────────────────────────
function Register-Connector {
    param([string]$JsonPath)

    $definition = Get-Content $JsonPath -Raw | ConvertFrom-Json
    $name       = $definition.name
    $cfg        = $definition.config

    # placeholder substitution (no credentials in the repo files)
    $json = ($definition.config | ConvertTo-Json -Depth 32 -Compress)
    $json = $json.Replace('__CONNECTOR_NAME__', $CxName)
    $json = $json.Replace('__DEBEZIUM_DB_HOST__', $DbHost)
    $json = $json.Replace('__POSTGRES_PORT_INTERNAL__', $DbPort)
    $json = $json.Replace('__POSTGRES_USER__', $SrcUser)
    $json = $json.Replace('__POSTGRES_PASSWORD__', $SrcPass)
    $json = $json.Replace('__POSTGRES_DB__', $SrcDb)
    $json = $json.Replace('__DEBEZIUM_TOPIC_PREFIX__', $DbPrefix)
    $json = $json.Replace('__DEBEZIUM_SLOT_NAME__', $DbSlot)
    $json = $json.Replace('__ANALYTICS_DB_USER__', $AnDbUser)
    $json = $json.Replace('__ANALYTICS_DB_PASSWORD__', $AnDbPass)
    $json = $json.Replace('__ANALYTICS_DB_NAME__', $AnDbName)
    $cfg = $json | ConvertFrom-Json

    Write-Host "── Registering connector '$name' ($([IO.Path]::GetFileName($JsonPath))) ..."

    $exists = $false
    try {
        $null = Invoke-Api "$KafkaConnectUrl/connectors/$name" -Method GET
        $exists = $true
    } catch { $exists = $false }

    if ($exists) {
        # PUT /connectors/{name}/config  -> replaces config in place (idempotent)
        $out = Invoke-Api "$KafkaConnectUrl/connectors/$name/config" -Method PUT -Body $cfg
        Write-Host "  existing connector found -> config replaced (PUT)."
    } else {
        $body = @{ name = $name; config = $cfg }
        $out  = Invoke-Api "$KafkaConnectUrl/connectors" -Method POST -Body $body
        Write-Host "  new connector created (POST)."
    }
    return $name
}

function Wait-ConnectorRunning {
    param([string]$Name, [int]$TimeoutSec = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $st = Invoke-Api "$KafkaConnectUrl/connectors/$Name/status" -Method GET
        $allOk = $true
        foreach ($t in $st.tasks) { if ($t.state -ne 'RUNNING' -and $t.state -ne 'UNASSIGNED') { $allOk = $false; $failedTask = $t } }
        if ($st.connector.state -ne 'RUNNING') { $allOk = $false }
        if ($allOk) {
            Write-Host "  connector '$Name' -> RUNNING, $((@($st.tasks)).Count) task(s) RUNNING."
            return $true
        }
        if ($st.connector.state -eq 'FAILED' -or ($failedTask -and $failedTask.state -eq 'FAILED')) {
            Write-Warning "  connector '$Name' FAILED:"
            Write-Warning (($st | ConvertTo-Json -Depth 8) | Out-String)
            return $false
        }
        Start-Sleep -Seconds 3
    }
    Write-Warning "  connector '$Name' did not reach RUNNING within ${TimeoutSec}s."
    return $false
}

function Show-AllStatuses {
    Write-Host "`n══ Connector status ══"
    $list = Invoke-Api "$KafkaConnectUrl/connectors" -Method GET
    foreach ($c in $list) {
        $st = Invoke-Api "$KafkaConnectUrl/connectors/$c/status" -Method GET
        $tasks = (@($st.tasks) | ForEach-Object { "$($_.state)" }) -join ','
        Write-Host ("  {0,-35} connector={1,-8} tasks=[{2}]" -f $c, $st.connector.state, $tasks)
        if ($st.connector.trace) { Write-Host "    trace: $($st.connector.trace)" }
    }
}

# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

if ($StatusOnly) {
    Wait-ConnectReady
    Show-AllStatuses
    exit 0
}

Wait-ConnectReady

# 1. Set Schema Registry global compatibility = BACKWARD (required by the
#    schema-evolution demo; also the default Confluent behavior made explicit).
try {
    $compat = Invoke-Api "$SchemaRegistryUrl/config" -Method PUT -Body @{ compatibility = 'BACKWARD' }
    Write-Host "Schema Registry global compatibility -> $($compat.compatibilityLevel)"
} catch {
    Write-Warning "Could not set Schema Registry compatibility (is it up?): $($_.Exception.Message)"
}

# 2. Debezium source (JSON CDC)
$registered = @()
$registered += Register-Connector -JsonPath (Join-Path $PSScriptRoot 'postgres-source.json')

# 3. JDBC sinks (Avro value) unless skipped
if (-not $SkipSinks) {
    $registered += Register-Connector -JsonPath (Join-Path $PSScriptRoot 'jdbc-sink-metrics.json')
    $registered += Register-Connector -JsonPath (Join-Path $PSScriptRoot 'jdbc-sink-alerts.json')
}

# 4. pre-seed Avro subjects
if ($RegisterSchemas) {
    Write-Host "`n── Pre-seeding Avro subjects under BACKWARD compatibility ..."
    $subjects = @{
        'application_submitted_events-value' = 'application_submitted_event.avsc'
        'application_submitted_dedup-value'  = 'application_submitted_dedup.avsc'
        'job_metrics_5m-value'               = 'job_metrics_5m.avsc'
        'application_alerts-value'           = 'application_alert.avsc'
    }
    foreach ($subject in $subjects.Keys) {
        $schemaJson = Get-Content (Join-Path $PSScriptRoot "..\schemas\$($subjects[$subject])") -Raw
        $schema = $schemaJson | ConvertFrom-Json
        # strip the free-text 'comment' property (not part of the Avro contract)
        $schema.PSObject.Properties.Remove('comment')
        $payload = @{ schemaType = 'AVRO'; schema = ($schema | ConvertTo-Json -Depth 32 -Compress) }
        try {
            $r = Invoke-Api "$SchemaRegistryUrl/subjects/$subject/versions" -Method POST -Body $payload
            Write-Host "  subject '$subject' -> version $($r.id)"
        } catch {
            if ("$($_.Exception.Message)" -match '409|Incompatib') {
                Write-Warning "  subject '$subject' registration rejected (compatibility) - check schemas/"
            } else {
                Write-Warning "  subject '$subject': $($_.Exception.Message)"
            }
        }
    }
}

Write-Host "`n── Waiting for connectors to reach RUNNING ..."
$allRunning = $true
foreach ($c in $registered) { if (-not (Wait-ConnectorRunning -Name $c)) { $allRunning = $false } }

Show-AllStatuses

if ($allRunning) {
    Write-Host "`n✅ All connectors registered and RUNNING."
    exit 0
} else {
    Write-Warning "`n⚠ Some connectors FAILED - inspect the status dump above and docs/troubleshooting.md."
    exit 1
}
