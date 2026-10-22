# Architecture - Real-Time Job Matching + Analytics Pipeline

## Business problems addressed

| # | Problem | Where it is solved |
|---|---------|--------------------|
| 1 | Closed jobs still recommend candidates (stale matches) | ksql invalidation legs + Spark intake filter + thin Delta invalidation writer |
| 2 | Job closes/fills/deletes must invalidate all recs fast | CDC -> Quix -> ksql stream-table legs -> `match_invalidation_events` -> Spark `match_invalidations` Delta |
| 3 | Duplicate "application submitted" events double-count | Quix stateful dedup with retention + DLQ |
| 4 | Freshness for recruiters (< 10 s) | measured p95 = 5996 ms (tests/06) on an end-to-end probe, not a synthetic benchmark |
| 5 | Live recruiter stats per job over rolling windows (5 min / 10 min hops) | ksqlDB tumbling (5 m) + hopping (10 m) windows -> Debezium JDBC sink -> analytics PostgreSQL |
| 6 | Spam/volume abuse detection | ksql alert@query on application counts per candidate -> `application_alerts` |
| 7 | Data loss/blame: malformed + duplicate events must be FULLY accounted | per-id reconciliation (tests/02): 116 in = 60 unique + 44 DUPLICATE + 12 BAD_WIRE_HEADER, 0 unaccounted |
| 8 | Topic/key/schema governance | explicit pinned partitions/keys, Schema Registry with BACKWARD strategy, per-subject documented evolution |

## Big-picture flow

```
Postgres (source: jobs/candidates/applications)
   |  Debezium (candidate-job-matching-cdc connector, JSON value, op=d/c/u/r)
   v
candidate_events / job_events / application_events    (Kafka, 3 partitions)
   |
   +---> Quix Streams stream-processor (docker, python, NO JAVA):
   |        * parse CDC -> project job_state_events / application_state_events
   |        * consume application_submitted_events
   |            - Confluent-wire Avro decode (fastavro on wire bytes)
   |            - stateful dedup on event_id (state pruned BEFORE dup check)
   |        * outputs: application_submitted_dedup (Avro id=1, exactly-once)
   |        * rejects -> dlq.application_submitted_events
   |                     (DUPLICATE / BAD_WIRE_HEADER / INVALID ...)
   |        * projection errors -> dlq.cdc_projection
   |
   +---> ksqlDB 7.6.1 (ksql/01..04.sql):
            * job_state / application_state KTABLEs (compacted topics)
            * match_invalidation_events  (closed/filled/deleted leg)
            * match_invalidation_events_app_leg  (JOIN prevention leg)
            * APPLICATIONS_BY_JOB / APP_DEDUP_WITH_JOB_STATE
            * job_metrics_5m  (5 m TUMBLING window CTAS - Avro id=10)
            * job_metrics_5m  (10 m HOPPING window CTAS - same topic+schema)
            * application_alerts (spam CTAS - Avro)
   |
   +---> Spark match engine (Realtime_Pipline/match_engine.py, pyspark+delta):
            * candidate_dim/job_dim CDC upserts (foreachBatch MERGE)
            * match_engine query: intake filter excludes
              closed/filled/deleted/removed jobs BEFORE scoring
            * match_invalidation_writer: thin streaming query persisting
              every invalidation to streaming_lake/match_invalidations
   |
   +---> Kafka Connect (Debezium JdbcSinkConnector 2.7.3):
            jdbc-sink-job-metrics-5m -> analytics.job_metrics_5m
            jdbc-sink-application-alerts -> analytics.application_alerts
            (PK = job_id + window_start + window_end -> upsert semantics)

Monitoring: Redpanda console :8080, Grafana+Prometheus (docker-compose.monitoring.yml),
scripts: monitoring/lag_report.ps1 / latency_report.py / load_test.ps1
```

## Design decisions an interviewer should probe

1. **JSON vs Avro on CDC topics**: CDC stays JSON so Spark/ksql stay schema-flexible
   and Debezium focus stays on change capture; the DEDUP layer is where Avro +
   Schema Registry (BACKWARD) earns its keep - that is the contract boundary
   with REST-style producers.
2. **`match_invalidation_events` stays JSON** because Spark consumes natively;
   forcing Avro would impose Schema-Registry integration on Spark (ABRIS)
   for zero benefit. `schemas/match_invalidation.avsc` is still registered
   as the documented contract so there is a compatible evolution path.
   NB ksqlDB uppercases unquoted identifiers: on the wire the keys are
   `JOB_ID`/`PRIOR_STATUS`/... and both consumers map that casing.
3. **Exactly-once where it matters**: the Quix processor runs exactly-once
   processing (changelog + checkpoints); the producer is an idempotent
   producer (acks=all, enable.idempotence). Downstream window aggregates
   are idempotent upserts (PK-replaying JDBC sink - tests/05 proves a
   replayed record neither duplicates nor double-counts).
4. **No Java code ships in the pipeline**: stream processing is Quix
   Streams (python) + ksqlDB SQL. The only Java-visible parts are pin'd
   community images (confluent 7.6.1 platform, Debezium 2.7.3 jars layered
   via docker/kafka-connect-extended.Dockerfile).
5. **DLQs are first-class**: every topic transformation that can reject
   records has its own DLQ topic and tests reconcile EVERY record per id.
6. **SQL window keys**: ksqlDB windowed aggregations carry the WINDOW key in
   the key's embedded structure; to make the value self-describing for the
   JDBC sink the queries project `LATEST_BY_OFFSET(job_id) AS JOB_ID_FLAT`
   (et al.) - this is documented in ksql/03 + README; Postgres window
   columns are BIGINT epoch ms because ksql WINDOWSTART/WINDOWEND are BIGINT
   and Debezium maps Avro long -> BIGINT.

## Failure modes covered by tests

| Failure | Test | Guard |
|---------|------|-------|
| Broker/connect/ksql/container down | 01 | pipeline won't pass health |
| Duplicated producer event | 02 | DUPLICATE DLQ + id accounting |
| Malformed Avro payload | 02 | BAD_WIRE_HEADER DLQ + accounting |
| Sink replay / restart double-write | 05 | PK upsert idempotency |
| Job closed while apps arrive | 03 | both ksql legs + Delta row |
| Stale job scored | intake filter (+ 03 sets it up) | filter before scoring |
| Slow pipeline violating SLA | 06 | p95 <= 10000 ms |
| Consumer groups falling behind | monitoring/lag_report.ps1 | lag threshold exit code |
