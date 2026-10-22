# tests/02 - Dedup + DLQ end-to-end integrity (data loss accounting).
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$repo = Split-Path -Parent (Split-Path -Parent $here)
$checks = @()
$passed = $true
$py = Join-Path $repo ".venv-streams\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }   # fallback: system python
$producer = Join-Path $repo "Realtime_Pipline\application_event_producer.py"

# 0. stream processor up (else nothing consumes)
$sp = docker inspect stream-processor --format "{{.State.Status}}" 2>$null
if ($sp -eq "running") { $checks += "OK stream-processor running" }
else { Write-Result -Name "02_dedup_dlq_integrity" -Passed $false -Checks @("FAIL stream-processor state=$sp"); exit 1 }

# 1. snapshot offsets (segment start)
Push-Location $here
& $py reconcile_segment.py record 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Pop-Location
    Write-Result -Name "02_dedup_dlq_integrity" -Passed $false -Checks @("FAIL could not record segment offsets")
    exit 1
}

# 2. inject real faults through the producer (30% replays, 15% malformed)
& $py $producer --rate 6 --duration 12 --seed $($PID) --duplicate-rate 0.3 --malformed-rate 0.15 2>&1 |
    Select-Object -Last 1 | ForEach-Object { $checks += "OK producer run: $_" }

# 3. wait for the processor to DRAIN the input segment: require committed
#    offsets == topic end (real zero lag), max 12 probes x 10s
$tries = 0
$drained = $false
while ($tries -lt 12) {
    Start-Sleep 10
    $tries++
    $in0 = (docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic-partitions "application_submitted_events:0" 2>$null)
    $inEnd = (($in0 -split ":")[-1] -as [int])
    $c0 = (docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --group app-submitted-dedup --describe 2>$null |
        Select-String "application_submitted_events" |
        ForEach-Object { if ($_ -match "application_submitted_events\s+\d+\s+(\d+)") { [int]$Matches[1] } } |
        Measure-Object -Sum).Sum
    if ($null -ne $inEnd -and $c0 -ge $inEnd) { $drained = $true; break }
}
if ($drained) { $checks += "OK processor drained input (lag=0) after $tries probe(s)" }
else {
    $checks += "WARN processor lag NOT zero after $tries probes (committed=$c0 end=$inEnd) - reconciliation may see unprocessed ids"
}


# 4. reconcile the segment: 100% accounting required
& $py reconcile_segment.py check 2>&1 | Tee-Object -Variable recon | Out-Null
$rc = $LASTEXITCODE
Pop-Location
if ($rc -eq 0) {
    $recon | ForEach-Object { $checks += "  $_" }
    Write-Result -Name "02_dedup_dlq_integrity" -Passed $true -Checks $checks
} else {
    $checks += "FAIL segment reconciliation:"
    $recon | ForEach-Object { $checks += "  $_" }
    Write-Result -Name "02_dedup_dlq_integrity" -Passed $false -Checks $checks
    exit 1
}
