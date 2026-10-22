## tests/04_windowed_metrics - expected output

Verifies the live recruiter statistics (business problem 5) landed in the
analytics PostgreSQL via the Debezium JDBC sink, with correct window math:

| Check | Expected |
|-------|----------|
| rows | `job_metrics_5m` has rows (materialized by ksql CTAS_JOB_METRICS_TUMBLING / _HOPPING) |
| window widths | `window_end - window_start` ∈ {300000ms = 5m tumbling, 600000ms = 10m hopping} only |
| both window types | at least one 5m row and (after enough history) one 10m row; missing 10m rows is a WARN if history < 10 min |
| PK discipline | zero duplicate `(job_id, window_start, window_end)` triples (upsert correctness) |
| sample | last rows with their counts printed as receipt |

Note: `window_start/window_end` are BIGINT epoch-millis on purpose - ksqlDB
exposes WINDOWSTART/WINDOWEND as BIGINT and Debezium maps Avro long to
Postgres BIGINT (documented in database/init_analytics.sql).
