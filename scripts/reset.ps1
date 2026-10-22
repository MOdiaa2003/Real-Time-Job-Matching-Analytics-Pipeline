# scripts/reset.ps1 - FULL teardown + rebuild of THIS pipeline demo.
#   docker compose down -v  ->  delete this project's containers AND its
#   named volumes (Kafka, both Postgres DBs) ->  clear Spark checkpoints +
#   Delta lake  ->  re-run the full setup (topics, connectors, ksql).
#
# Safety: scoped by the compose project in THIS directory only; the
# preserved containers renamed *_old_jobposting (previous project) are
# never touched. Optional -KeepSparkState skips deleting checkpoints/lake.
param([switch]$KeepSparkState)
$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repo

Write-Host "== stopping and removing THIS project's containers + volumes"
docker compose down -v

if (-not $KeepSparkState) {
    foreach ($dir in @("$repo\checkpoint", "$repo\streaming_lake")) {
        if (Test-Path $dir) {
            Remove-Item -LiteralPath $dir -Recurse -Force
            Write-Host "cleared $dir"
        }
    }
}

Write-Host "== re-running setup (venvs kept, images kept)"
& powershell -ExecutionPolicy Bypass -File (Join-Path $repo "scripts\setup.ps1")
Write-Host "RESET COMPLETE. Everything rebuilds from scratch on the next run."
