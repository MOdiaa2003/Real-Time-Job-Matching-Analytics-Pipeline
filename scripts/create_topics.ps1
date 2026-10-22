# ═══════════════════════════════════════════════════════════════════════
# create_topics.ps1 - idempotent topic creation
# Broker has auto.create=false on purpose: every topic's PARTITION COUNT
# and retention are decided EXPLICITLY here (business problem 8), so a
# mis-keyed accidental write can never create a wrong-layout topic.
# Partition rationale (also documented in docs/topics_and_keys.md):
#   * source CDC + dedup + output aggregates: 3 partitions (keyed by
#     entity id; scales 3 consumer instances; local demo friendly)
#   * DLQs: 1 partition (diagnostic traffic, order for humans)
#   * tables (job_state_events/application_state_events): COMPACTED so
#     ksqlDB materialization matches table semantics w/o unbounded growth
# ═══════════════════════════════════════════════════════════════════════
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $repoRoot '.env'
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=\s]+)\s*=\s*(.*)\s*$') {
            $k = $matches[1]
            $v = ($matches[2] -replace '\s+#.*$', '').Trim()
            if (-not (Get-Item "env:$k" -ErrorAction SilentlyContinue)) { Set-Item "env:$k" $v }
        }
    }
}

$kafkaContainer = 'kafka'
$bootstrapInternal = 'localhost:29092'
$partitions = if ($env:TOPIC_PARTITIONS) { $env:TOPIC_PARTITIONS } else { 3 }

function New-Topic {
    param([string]$Name, [int]$P = 3, [string]$Extra = '')
    # idempotent: --if-not-exists
    $args = @('/opt/kafka/bin/kafka-topics.sh', '--create',
              "--bootstrap-server=$bootstrapInternal", '--if-not-exists',
              "--topic=$Name", "--partitions=$P", '--replication-factor=1')
    if ($Extra) {
        $Extra -split ',' | ForEach-Object { $args += "--config=$_" }
    } else {
        $args += '--config=retention.ms=172800000'  # 48h default for event topics
    }
    $out = docker exec $kafkaContainer @args 2>&1
    Write-Host ("  {0,-38} partitions={1} {2}" -f $Name, $P, (($out | Select-Object -First 1) -replace "^Topic '\w+'.*",""))
}

Write-Host "── Creating topics on $kafkaContainer ..."

# CDC source topics (Debezium writes them)
New-Topic 'candidate_events'     -P $partitions
New-Topic 'job_events'           -P $partitions
New-Topic 'application_events'   -P $partitions

# web-app + dedup
New-Topic 'application_submitted_events' -P $partitions
New-Topic 'application_submitted_dedup'  -P $partitions

# flat projections feeding ksqlDB (compacted - KTable semantics)
New-Topic 'job_state_events'       -P $partitions -Extra 'cleanup.policy=compact,min.compaction.lag.ms=60000,retention.ms=-1'
New-Topic 'application_state_events' -P $partitions -Extra 'cleanup.policy=compact,min.compaction.lag.ms=60000,retention.ms=-1'

# stream outputs
New-Topic 'match_invalidation_events' -P $partitions
New-Topic 'job_metrics_5m'            -P $partitions
New-Topic 'application_alerts'        -P $partitions

# DLQ topics (single partition: diagnostic streams keep order for humans)
New-Topic 'dlq.application_submitted_events' -P 1
New-Topic 'dlq.cdc_projection'               -P 1
New-Topic 'dlq.jdbc_sink_metrics'            -P 1
New-Topic 'dlq.jdbc_sink_alerts'             -P 1

# Debezium heartbeat topic (heartbeat.interval.ms is ON - the connector
# writes to __debezium-heartbeat.<prefix> and auto-create is off)
New-Topic '__debezium-heartbeat.talent' -P 1 -Extra 'retention.ms=3600000,cleanup.policy=delete'

# ── ksql internal topics (explicit; auto-creation OFF on the broker) ──
Write-Host "── ksqlDB internal topics ..."
New-Topic '_confluent-ksql-job_matching__command_topic' -P 1 -Extra 'retention.ms=-1,cleanup.policy=compact'
New-Topic '_confluent-command' -P 1 -Extra 'retention.ms=-1,cleanup.policy=compact'
New-Topic '_confluent-metadata' -P 1 -Extra 'retention.ms=-1,cleanup.policy=compact'
New-Topic '_confluent-license' -P 1 -Extra 'retention.ms=-1,cleanup.policy=compact'
New-Topic '_confluent-ksql-job_matching__transaction_state' -P 1 -Extra 'retention.ms=-1,cleanup.policy=compact'
New-Topic '_confluent_ksql_audit_events' -P 1 -Extra 'retention.ms=60000,cleanup.policy=delete'

Write-Host "`n── Final topic list:"
docker exec $kafkaContainer /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server=$bootstrapInternal
