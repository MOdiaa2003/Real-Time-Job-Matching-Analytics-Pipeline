# monitoring/lag_report.ps1 - consumer-group lag snapshot for every
# pipeline consumer group. Exit code is 1 if any group lags more than
# -MaxLag messages (default: 20) so this can gate CI or alerting.
param([int]$MaxLag = 20)

$rows = docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --all-groups --describe 2>$null |
    Where-Object { $_ -match "^(app-|connect-|ksql_|application|spark)" -or $_ -match "^\S+\s+\S+\s+\S+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(\S+)" }

# kafka-consumer-groups columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
$report = @()
foreach ($line in ($rows | Where-Object { $_ -notmatch "^Group$" })) {
    if ($line -match "^(\S+)\s+(\S+)\s+(\d+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\S+)") {
        $g = $Matches[1]; $t = $Matches[2]; $lag = [int]$Matches[6]
        $client = if ($Matches[7]) { $Matches[7].Split("-")[0] } else { "" }
        $report += [pscustomobject]@{ Group = $g; Topic = $t; Lag = $lag }
    }
}

if (-not $report.Count) {
    Write-Host "no consumer-group rows found (broker up?)"
    exit 1
}

$report | Sort-Object Group, Topic, Lag | Format-Table -AutoSize |
    Tee-Object -Variable rendered | Out-Host
$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$worst = ($report | Measure-Object Lag -Maximum).Maximum
$lags = ($report | Measure-Object Lag -Sum).Sum

$line = "[{0}] lag_report: groups={1} total_lag={2} max_lag={3}" -f `
    $stamp, ($report.Group | Sort-Object -Unique).Count, $lags, $worst
Add-Content -Path (Join-Path $PSScriptRoot "lag_report.jsonl") -Value $line
Write-Host $line

if ($worst -gt $MaxLag) {
    Write-Host "FAIL max lag $worst exceeds threshold $MaxLag"
    exit 1
}
Write-Host "PASS within threshold $MaxLag"
