# tests/01 - Pipeline startup & health smoke.
# Verifies the whole stack is up, reachable and correctly wired:
# containers, topics, connectors, ksql persistent queries, analytics DB.
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$global:SUITE_HERE = $here
. (Join-Path (Split-Path -Parent $here) "_common.ps1")

$checks = @()
$passed = $true

# 1. containers healthy
$required = @("kafka", "schema-registry", "kafka-connect", "ksqldb",
              "my_postgres", "my_postgres_analytics", "stream-processor")
foreach ($c in $required) {
    $state = docker inspect $c --format "{{.State.Status}}" 2>$null
    $health = docker inspect $c --format "{{.State.Health.Status}}" 2>$null
    $ok = ($state -eq "running") -and (($health -eq "healthy") -or ($health -eq "") -or ($null -eq $health) -or ($health -eq "<nil>"))
    if ($ok) { $checks += "OK container $c running/healthy" }
    else { $checks += "FAIL container $c state=$state health=$health"; $passed = $false }
}

# 2. topics exist
$topics = docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list 2>$null
$must = @("candidate_events", "job_events", "application_events", "application_submitted_events",
          "application_submitted_dedup", "application_state_events", "job_state_events",
          "match_invalidation_events", "job_metrics_5m", "application_alerts",
          "dlq.application_submitted_events", "dlq.cdc_projection",
          "dlq.jdbc_sink_metrics", "dlq.jdbc_sink_alerts")
foreach ($t in $must) {
    if ($topics -contains $t) { $checks += "OK topic $t exists" }
    else { $checks += "FAIL topic $t missing"; $passed = $false }
}

# 3. connectors RUNNING
foreach ($c in @("candidate-job-matching-cdc", "jdbc-sink-job-metrics-5m", "jdbc-sink-application-alerts")) {
    if (Test-ConnectorRunning $c) { $checks += "OK connector $c RUNNING" }
    else { $checks += "FAIL connector $c not RUNNING"; $passed = $false }
}

# 4. ksql persistent queries RUNNING
$ok = Test-AllKsqlQueriesRunning
if ($ok) { $checks += "OK ksql persistent queries RUNNING" }
else { $checks += "FAIL ksql persistent queries not RUNNING"; $passed = $false }

# 5. analytics tables exist with correct window types
$types = Invoke-AnalyticsPsql "SELECT data_type FROM information_schema.columns WHERE table_name='job_metrics_5m' AND column_name='window_start';"
if ($types -eq "bigint") { $checks += "OK analytics job_metrics_5m.window_start is BIGINT" }
else { $checks += "FAIL analytics job_metrics_5m.window_start = '$types' (expected bigint)"; $passed = $false }

$tables = (Invoke-AnalyticsPsql "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='public';")
foreach ($t in @("job_metrics_5m", "application_alerts", "job_current_state")) {
    if ($tables -match $t) { $checks += "OK analytics table $t exists" }
    else { $checks += "FAIL analytics table $t missing"; $passed = $false }
}

# 6. schema registry reachable + subject registered
try {
    $subjects = curl.exe -sS http://localhost:8081/subjects 2>$null
    if ($subjects -match "job_metrics_5m-value|application_submitted_events-value") {
        $checks += "OK schema registry reachable, key subjects registered"
    } else { $checks += "FAIL schema registry subjects: $subjects"; $passed = $false }
} catch { $checks += "FAIL schema registry unreachable"; $passed = $false }

Write-Result -Name "01_pipeline_health" -Passed $passed -Checks $checks
if (-not $passed) { exit 1 }
