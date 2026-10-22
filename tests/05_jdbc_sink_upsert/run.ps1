# tests/05 - JDBC sink upsert idempotency (replay-safe PK writes).
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$checks = @()
$repo = Split-Path -Parent (Split-Path -Parent $here)
$py = Join-Path $repo ".venv-streams\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }   # fallback: system python

if (Test-ConnectorRunning "jdbc-sink-job-metrics-5m") {
    $checks += "OK connector jdbc-sink-job-metrics-5m RUNNING"
} else {
    Write-Result -Name "05_jdbc_sink_upsert" -Passed $false -Checks @("FAIL connector not RUNNING")
    exit 1
}

Push-Location $here
& $py replay_check.py 2>&1 | Tee-Object -Variable out
Pop-Location
if ($LASTEXITCODE -eq 0) {
    $checks += $out | Where-Object { $_ -match "^OK|PK rows" }
    Write-Result -Name "05_jdbc_sink_upsert" -Passed $true -Checks $checks
} else {
    $checks += $out | Where-Object { $_ -match "^OK|^FAIL|PK rows" } | Select-Object -First 6
    Write-Result -Name "05_jdbc_sink_upsert" -Passed $false -Checks $checks
    exit 1
}
