## tests/01_pipeline_health - expected output

What the suite verifies and what a healthy stack looks like:

| # | Check | Expected |
|---|-------|----------|
| 1 | Containers | `kafka`, `schema-registry`, `kafka-connect`, `ksqldb`, `my_postgres`, `my_postgres_analytics`, `stream-processor` all `running` + `healthy` where a healthcheck exists |
| 2 | Topics | all CDC topics (`candidate_events`, `job_events`, `application_events`), dedup topics (`application_submitted_events`, `application_submitted_dedup`), ksql outputs (`job_state_events`, `match_invalidation_events`, `job_metrics_5m`, `application_alerts`), DLQ (`dlq.application_submitted_events`) and `high_priority_matches` exist |
| 3 | Connectors | `candidate-job-matching-cdc`, `jdbc-sink-job-metrics-5m`, `jdbc-sink-application-alerts` all `RUNNING` (connector AND tasks) |
| 4 | ksqlDB | all persistent queries report state `RUNNING` |
| 5 | Analytics DB | `job_metrics_5m.window_start` is `bigint` (epoch-millis, matches the ksql/Debezium contract); tables `job_metrics_5m`, `application_alerts`, `job_current_state` exist |
| 6 | Schema Registry | reachable; key subjects (`application_submitted_events-value`, `job_metrics_5m-value`) registered |

Concretely (from the real Stage-4 acceptance run):

```
[2026-10-22 ...] 01_pipeline_health : PASS
  - OK container kafka running/healthy
  - OK container schema-registry running/healthy
  - OK container kafka-connect running/healthy
  - OK container ksqldb running/healthy
  - OK container my_postgres running/healthy
  - OK container my_postgres_analytics running/healthy
  - OK container stream-processor running/healthy
  - OK topic candidate_events exists
  - ...
  - OK connector candidate-job-matching-cdc RUNNING
  - ...
  - OK ksql persistent queries RUNNING
  - OK analytics job_metrics_5m.window_start is BIGINT
  - ...
```
