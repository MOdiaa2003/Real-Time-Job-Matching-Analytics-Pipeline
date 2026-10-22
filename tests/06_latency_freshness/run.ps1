# tests/06 - End-to-end freshness of the real-time event path (10s SLA).
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$checks = @()
$repo = Split-Path -Parent (Split-Path -Parent $here)
$py = Join-Path $repo ".venv-streams\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }   # fallback: system python

$sp = docker inspect stream-processor --format "{{.State.Status}}" 2>$null
if ($sp -eq "running") { $checks += "OK stream-processor running" }
else { Write-Result -Name "06_latency_freshness" -Passed $false -Checks @("FAIL stream-processor state=$sp"); exit 1 }

Push-Location $here
& $py latency_probe.py 2>&1 | Tee-Object -Variable out
Pop-Location
if ($LASTEXITCODE -eq 0) {
    $checks += $out | Where-Object { $_ -match "^OK|measured|PASS" }
    Write-Result -Name "06_latency_freshness" -Passed $true -Checks $checks
} else {
    $checks += $out | Where-Object { $_ -match "measured|^FAIL|PASS" } | Select-Object -First 5
    Write-Result -Name "06_latency_freshness" -Passed $false -Checks $checks
    exit 1
}
