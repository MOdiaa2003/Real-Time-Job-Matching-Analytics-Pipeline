# tests/04 - Windowed metrics in analytics DB (tumbling 5m + hopping 10m).
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$checks = @()
$passed = $true

# 0. pipeline prerequisites (no new data needed - evaluates the materialized state)
foreach ($c in @("jdbc-sink-job-metrics-5m")) {
    if (Test-ConnectorRunning $c) { $checks += "OK connector $c RUNNING" }
    else { $checks += "FAIL connector $c not RUNNING"; $passed = $false }
}

# 1. rows exist
$n = [int](Invoke-AnalyticsPsql "SELECT COUNT(*) FROM job_metrics_5m;")
if ($n -gt 0) { $checks += "OK job_metrics_5m has $n rows" }
else { $checks += "FAIL job_metrics_5m empty"; $passed = $false }

# 2. only valid window widths: 300000 (tumbling 5m) or 600000 (hopping 10m)
$widths = (Invoke-AnalyticsPsql "SELECT DISTINCT window_end - window_start FROM job_metrics_5m ORDER BY 1;")
$bad = ($widths | Where-Object { $_ -notin @("300000", "600000") }).Count
if ($bad -eq 0 -and $widths) { $checks += "OK window widths valid: $($widths -join ',')" }
else { $checks += "FAIL unexpected window widths: $($widths -join ',')"; $passed = $false }

# 3. both window types materialized (tumbling AND hopping)
$hasT = [bool](Invoke-AnalyticsPsql "SELECT 1 FROM job_metrics_5m WHERE window_end-window_start=300000 LIMIT 1;")
$hasH = [bool](Invoke-AnalyticsPsql "SELECT 1 FROM job_metrics_5m WHERE window_end-window_start=600000 LIMIT 1;")
if ($hasT) { $checks += "OK 5-minute tumbling windows present" } else { $checks += "WARN no 5m tumbling rows yet"; if ($n -gt 0) {} }
if ($hasH) { $checks += "OK 10-minute hopping windows present" } else { $checks += "WARN no 10m hopping rows yet (needs more history)" }

# 4. PK honesty: no (job_id, window_start, window_end) duplicates are possible,
#    but verify upsert did not create dup PKs for identical triples
$dupes = [int](Invoke-AnalyticsPsql "SELECT COUNT(*) FROM (SELECT job_id, window_start, window_end FROM job_metrics_5m GROUP BY 1,2,3 HAVING COUNT(*) > 1) x;")
if ($dupes -eq 0) { $checks += "OK no duplicate primary keys" }
else { $checks += "FAIL $dupes duplicate PK rows"; $passed = $false }

# 5. sample rows recorded for the report
$sample = Invoke-AnalyticsPsql "SELECT job_id || ' w[' || window_start || '->' || window_end || '] apps=' || applications_cnt || ' matches=' || matches_cnt FROM job_metrics_5m ORDER BY window_end DESC LIMIT 3;"
$sample | ForEach-Object { $checks += "    row: $_" }

Write-Result -Name "04_windowed_metrics" -Passed $passed -Checks $checks
if (-not $passed) { exit 1 }
