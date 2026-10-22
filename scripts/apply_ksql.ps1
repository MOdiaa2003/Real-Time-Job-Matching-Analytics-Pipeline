# =========================================================================
# apply_ksql.ps1 - applies ksql/*.sql in order to the ksqlDB server.
# Pure ASCII on purpose: PS 5.1 decodes BOM-less files as cp1252 and
# multi-byte symbols can break parsing on some consoles.
# Idempotent-ish: CREATE statements that already exist are skipped.
# =========================================================================
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot '.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=\s]+)\s*=\s*(.*)\s*$') {
            $k = $matches[1]
            $v = ($matches[2] -replace '\s+#.*$', '').Trim()
            if (-not (Get-Item "env:$k" -ErrorAction SilentlyContinue)) { Set-Item "env:$k" $v }
        }
    }
}

$KSqlUrl = $env:KSQLDB_URL; if (-not $KSqlUrl) { $KSqlUrl = 'http://localhost:8088' }
$MetricsWindow = if ($env:METRICS_WINDOW_MINUTES) { $env:METRICS_WINDOW_MINUTES } else { 5 }
$Grace = if ($env:METRICS_GRACE_MINUTES) { $env:METRICS_GRACE_MINUTES } else { 2 }
$ApplyLimit = if ($env:ALERT_APPLY_LIMIT) { $env:ALERT_APPLY_LIMIT } else { 5 }

function Wait-KSql {
    Write-Host "Waiting for ksqlDB at $KSqlUrl ..."
    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-RestMethod -Uri "$KSqlUrl/info" -Method GET -TimeoutSec 5
            Write-Host ("  ksqlDB v{0} ready." -f $r.version)
            return
        } catch { Start-Sleep -Seconds 4 }
    }
    throw "ksqlDB not reachable after 180s."
}

function Invoke-KSql {
    param([string]$Sql, [string]$Label)
    $body = @{
        ksql = $Sql
        streamsProperties = @{ 'ksql.streams.auto.offset.reset' = 'earliest' }
    } | ConvertTo-Json -Depth 6 -Compress
    # curl.exe instead of Invoke-RestMethod: PS 5.1 does not surface the
    # response body of failed ksql requests reliably (empty GetResponseStream).
    $tmp = [IO.Path]::GetTempFileName()
    [IO.File]::WriteAllBytes($tmp, [Text.Encoding]::UTF8.GetBytes($body))
    $out = & curl.exe -sS -w "`n%{http_code}" -X POST `
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" `
        --data-binary "@$tmp" "$KSqlUrl/ksql"
    Remove-Item $tmp -ErrorAction SilentlyContinue
    $lines = @($out | Where-Object { $_ -is [string] })
    $httpCode = $lines[-1]
    $jsonText = ($lines[0..($lines.Count - 2)] -join "`n")
    if ($httpCode -ne '200') {
        if ($jsonText -match 'same name already exists|already registered|already exists') {
            Write-Host ("  SKIP {0} (already exists)" -f $Label)
            return $true
        }
        Write-Warning ("  FAIL {0}: HTTP $httpCode" -f $Label)
        if ($jsonText) {
            Write-Warning ("    " + ($jsonText.Substring(0, [Math]::Min(1200, $jsonText.Length))))
        }
        return $false
    }
    # HTTP 200: check each returned command status for ERROR (e.g. same-name DDL)
    if ($jsonText -match '"status":"ERROR"') {
        Write-Host ("  SKIP {0} (already registered / rejected by ksqlDB)" -f $Label)
        return $true
    }
    Write-Host ("  OK   {0}" -f $Label)
    return $true
}

Wait-KSql

$sqlFiles = Get-ChildItem "$repoRoot\ksql\*.sql" | Sort-Object Name
$anyFail = $false
foreach ($f in $sqlFiles) {
    Write-Host "-- Applying $($f.Name) ..."
    $raw = Get-Content $f.FullName -Raw
    $statements = ($raw -replace '--[^\r\n]*', '') -split ';\s*' |
        Where-Object { $_.Trim() -ne '' }
    foreach ($st in $statements) {
        $stmt = $st.Trim()
        # ksqlDB's parser requires the statement-terminating semicolon even
        # when statements are submitted one at a time
        $stmt = $stmt + ';'
        if ($stmt -match '^SET ') { continue }  # session property; applied via streamsProperties below
        # window/alert templates -> environment-driven values
        $stmt = $stmt -replace 'SIZE 5 MINUTES', "SIZE $MetricsWindow MINUTES"
        $stmt = $stmt -replace 'GRACE PERIOD 2 MINUTES', "GRACE PERIOD $Grace MINUTES"
        $stmt = $stmt -replace 'HAVING COUNT\(\*\) > 5', "HAVING COUNT(*) > $ApplyLimit"
        $first = ($stmt -split "\s+" | Select-Object -First 3) -join ' '
        if (-not (Invoke-KSql -Sql $stmt -Label $first)) {
            $anyFail = $true
            break
        }
    }
    if ($anyFail) {
        Write-Warning ("File {0} had failures - check statements above." -f $f.Name)
        exit 1
    }
}

Write-Host "`n-- Active ksqlDB queries:"
try {
    $resp = Invoke-RestMethod -Uri "$KSqlUrl/ksql" -Method POST `
        -ContentType 'application/vnd.ksql.v1+json; charset=utf-8' `
        -Body '{"ksql":"SHOW QUERIES;"}' -TimeoutSec 30
    foreach ($r in @($resp)) {
        if ($r.queries) { $r | ConvertTo-Json -Depth 6 | Out-Host }
    }
    Write-Host "-- done"
} catch { Write-Warning "SHOW QUERIES failed: $($_.Exception.Message)" }
