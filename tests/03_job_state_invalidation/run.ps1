# tests/03 - Job-state invalidation (closed job -> stale recs die).
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$checks = @()
$repo = Split-Path -Parent (Split-Path -Parent $here)
$py = Join-Path $repo ".venv-streams\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }   # fallback: system python

Push-Location $here
& $py verify_invalidation.py --spark 2>&1 | Tee-Object -Variable out
Pop-Location
if ($LASTEXITCODE -eq 0) {
    $checks = $out | Where-Object { $_ -match "^OK|^PASS|     job_id=" }
    Write-Result -Name "03_job_state_invalidation" -Passed $true -Checks $checks
} else {
    $checks = $out | Where-Object { $_ -match "^FAIL|FAIL no|FAIL source|     " } | Select-Object -First 8
    Write-Result -Name "03_job_state_invalidation" -Passed $false -Checks $checks
    exit 1
}
