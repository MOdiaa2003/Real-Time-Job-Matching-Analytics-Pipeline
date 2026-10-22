# _common.ps1 - shared helpers for all acceptance test suites.
# Each suite's run.ps1 dot-sources this file. Pure ASCII on purpose
# (PowerShell 5.1 mis-decodes multibyte in BOM-less .ps1 files).

# NB: $PSScriptRoot inside a dot-sourced file is THIS file's dir (tests\),
# not the suite's dir - so suites set $global:SUITE_HERE before dot-sourcing
# and Write-Result writes receipts next to the suite that ran.
if (-not $global:SUITE_HERE) { $global:SUITE_HERE = $PSScriptRoot }

function Write-Result {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][bool]$Passed,
        [Parameter(Mandatory = $true)][string[]]$Checks,
        [Parameter(Mandatory = $false)][string]$Note = ""
    )
    $outDir = if ($global:SUITE_HERE) { Join-Path $global:SUITE_HERE "results" }
              else { Join-Path $PSScriptRoot "results" }
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    $stamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $verdict = "FAIL"
    if ($Passed) { $verdict = "PASS" }

    $lines = @()
    $lines += ("[{0}] {1} : {2}" -f $stamp, $Name, $verdict)
    foreach ($c in $Checks) { $lines += "  - $c" }
    if ($Note) { $lines += "  note: $Note" }
    $text = ($lines -join "`r`n")
    # console + file
    Write-Host $text
    Set-Content -Path (Join-Path $outDir "last_run.txt") -Value $text -Encoding ASCII
    Set-Content -Path (Join-Path $outDir ("{0}.txt" -f (Get-Date -Format "yyyyMMdd_HHmmss"))) -Value $text -Encoding ASCII
    return $Passed
}

function Invoke-AnalyticsPsql {
    param([Parameter(Mandatory = $true)][string]$Sql)
    docker exec my_postgres_analytics psql -U postgres -d analytics -t -A -c $Sql 2>$null
}

function Invoke-SourcePsql {
    param([Parameter(Mandatory = $true)][string]$Sql)
    docker exec my_postgres psql -U postgres -d recruitment -t -A -c $Sql 2>$null
}

function Get-TopicEndOffsets {
    # returns hashtable topic -> total offset end (sum across partitions)
    param([Parameter(Mandatory = $true)][string[]]$Topics)
    $list = ($Topics | ForEach-Object { "$($_):0" }) -join ","
    # NOTE: this sums partition 0 only; suites that need all partitions use
    # the python helper instead (windowed topics use 3 partitions).
    $raw = docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic-partitions $list 2>$null
    $map = @{}
    foreach ($line in $raw) {
        if ($line -match "^([^:]+):(\d+):(\d+)$") {
            $t = $Matches[1]; $p = [int]$Matches[2]; $o = [int]$Matches[3]
            if (-not $map.ContainsKey($t)) { $map[$t] = 0 }
            $map[$t] += $o
        }
    }
    return $map
}

function Test-ConnectorRunning {
    param([Parameter(Mandatory = $true)][string]$Name)
    try {
        $s = Invoke-RestMethod -Uri "http://localhost:8083/connectors/$Name/status" -TimeoutSec 10
        $ok = $s.connector.state -eq "RUNNING"
        foreach ($t in $s.tasks) { if ($t.state -ne "RUNNING") { $ok = $false } }
        return $ok
    } catch { return $false }
}

function Test-AllKsqlQueriesRunning {
    # NB: curl.exe in PS 5.1 strips embedded quotes from --data-binary args,
    # sending ksql invalid JSON; Invoke-RestMethod avoids the whole class.
    try {
        $r = Invoke-RestMethod -Uri "http://localhost:8088/ksql" -Method Post `
                -ContentType "application/vnd.ksql.v1+json; charset=utf-8" `
                -Body '{"ksql":"SHOW QUERIES;"}' -TimeoutSec 15
    } catch { return $false }
    $persistent = @()
    foreach ($resp in @($r)) {
        foreach ($q in @($resp.queries)) { $persistent += $q }
    }
    $persistent = @($persistent | Where-Object { $_.queryType -eq "PERSISTENT" })
    $bad = @($persistent | Where-Object { $_.state -ne "RUNNING" })
    return ($persistent.Count -gt 0 -and $bad.Count -eq 0)
}
