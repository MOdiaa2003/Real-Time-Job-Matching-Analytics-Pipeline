# monitoring/load_test.ps1 - load generator driver.
# Runs the synthetic producers at a given rate for a duration, then prints
# the resulting offsets of every pipeline output topic for quick eyeballing.
param(
    [double]$Rate = 20,          # application events/sec
    [int]$DurationSec = 30,
    [double]$DuplicateRate = 0.05,
    [double]$MalformedRate = 0.03,
    [switch]$SkipGenerator       # set to only run the application producer
)

$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$pyS = Join-Path $repo ".venv-streams\Scripts\python.exe"   # kafka producer deps
if (-not (Test-Path $pyS)) { $pyS = "python" }
$pyG = Join-Path $repo ".venv-gen\Scripts\python.exe"       # generator deps (psycopg2)
if (-not (Test-Path $pyG)) { $pyG = "python" }

$env:PYTHONIOENCODING = "utf-8"
if (-not $SkipGenerator) {
    Write-Host "starting source lifecycle generator (rate knobs at defaults)..."
    $gen = Start-Process -FilePath $pyG `
        -ArgumentList "`"$repo\Realtime_Pipline\recruitment_lifecycle_generator.py`"" `
        -WorkingDirectory $repo -NoNewWindow -PassThru `
        -RedirectStandardOutput "$env:TEMP\loadtest_gen.log"
    Start-Sleep 3
}

Write-Host ("producing application events: rate={0}/s duration={1}s dup={2} malformed={3}" -f `
    $Rate, $DurationSec, $DuplicateRate, $MalformedRate)
& $pyS (Join-Path $repo "Realtime_Pipline\application_event_producer.py") `
    --rate $Rate --duration $DurationSec --seed 7 `
    --duplicate-rate $DuplicateRate --malformed-rate $MalformedRate

Start-Sleep 10  # let the processor drain

$topics = @("application_submitted_events", "application_submitted_dedup",
            "dlq.application_submitted_events", "job_state_events",
            "match_invalidation_events", "job_metrics_5m", "application_alerts")
$list = ($topics | ForEach-Object { "$($_):0" }) -join ","
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic-partitions $list 2>$null

Add-Content -Path (Join-Path $PSScriptRoot "load_test.jsonl") -Value `
    ("[{0}] load test done rate={1}/s dur={2}s" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Rate, $DurationSec)
