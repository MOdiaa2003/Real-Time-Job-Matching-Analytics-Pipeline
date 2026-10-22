# scripts/run.ps1 - day-to-day pipeline start (after scripts/setup.ps1 once).
#   * makes sure containers are up
#   * starts the Spark match engine + synthetic producers as BACKGROUND
#     processes with per-process logs under logs\ (targeted process match
#     by command line - never blanket-kills all python)
#   * prints the live URLs to watch
param(
    [switch]$NoEngine,      # skip the Spark match engine
    [switch]$WithGenerator, # also start the source lifecycle generator
    [switch]$WithProducer   # also start the application event producer
)
$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repo
New-Item -ItemType Directory -Force -Path "$repo\logs" | Out-Null

# 1. containers
docker compose up -d | Out-Null
$deadline = (Get-Date).AddMinutes(5)
do {
    Start-Sleep 5
    $bad = docker compose ps 2>$null | Select-String -Pattern "unhealthy|restarting" -Quiet
} while ($bad -and (Get-Date) -lt $deadline)

# 2. match engine (only if not already running)
if (-not $NoEngine) {
    $running = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match "match_engine\.py" }
    if ($running) {
        Write-Host "match engine: SKIP (already running, pids $($running.ProcessId -join ','))"
    } else {
        Start-Process -FilePath "python" -ArgumentList "$repo\Realtime_Pipline\match_engine.py" `
            -WorkingDirectory $repo -WindowStyle Hidden `
            -RedirectStandardOutput "$repo\logs\match_engine.log" `
            -RedirectStandardError "$repo\logs\match_engine.err.log"
        Write-Host "match engine: STARTED (logs\match_engine.log) - needs ~2-3 min warmup"
    }
}

# 3. optional producers
if ($WithGenerator) {
    $gen = ".venv-streams\Scripts\python.exe"
    Start-Process -FilePath $gen -ArgumentList "$repo\Realtime_Pipline\recruitment_lifecycle_generator.py" `
        -WorkingDirectory $repo -WindowStyle Hidden `
        -RedirectStandardOutput "$repo\logs\generator.log" `
        -RedirectStandardError "$repo\logs\generator.err.log"
    Write-Host "generator: STARTED (logs\generator.log)"
}
if ($WithProducer) {
    $gen = ".venv-streams\Scripts\python.exe"
    Start-Process -FilePath $gen -ArgumentList "$repo\Realtime_Pipline\application_event_producer.py --rate 10 --duration 999999" `
        -WorkingDirectory $repo -WindowStyle Hidden `
        -RedirectStandardOutput "$repo\logs\producer.log" `
        -RedirectStandardError "$repo\logs\producer.err.log"
    Write-Host "producer: STARTED (logs\producer.log)"
}

Write-Host ""
Write-Host "Pipeline running. Watch:"
Write-Host "  Redpanda console : http://localhost:8084"
Write-Host "  ksqlDB           : http://localhost:8088"
Write-Host "  Kafka Connect    : http://localhost:8083"
Write-Host "  Grafana          : http://localhost:3000 (docker compose -f docker-compose.monitoring.yml up -d)"
Write-Host "Stop with: scripts\stop.ps1"
