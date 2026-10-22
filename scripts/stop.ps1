# scripts/stop.ps1 - graceful demo shutdown.
#   * stops ONLY the pipeline helper processes started by scripts\run.ps1
#     (matched by command line, never a blanket python kill)
#   * docker compose stop (NOT down -v: data in Postgres/Kafka volumes and
#     the Spark checkpoints survive, restart with scripts\run.ps1)
$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repo

foreach ($pat in @("match_engine\.py", "recruitment_lifecycle_generator\.py",
                   "application_event_producer\.py")) {
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match $pat }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "stopped helper process $($p.ProcessId) ($pat)"
    }
}

docker compose stop
Write-Host "containers stopped (volumes kept). scripts\run.ps1 to resume."
