-- â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
-- 03_windowed_metrics.sql
-- Business problem 5 (live recruiter statistics):
--   EVENT-TIME windowed aggregation (window.end computed from
--   event_time_ms on the dedup stream, NOT wall-clock),
--   with a GRACE PERIOD so late-arriving events still count.
--   * 5-minute TUMBLING window
--   * 10-minute HOPPING window (advance 5 m)
-- Both write to topic job_metrics_5m (job_id, window_start/end,
-- applications_cnt, matches_cnt) consumed by the JDBC sink -> PostgreSQL.
-- â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

-- Enrich once (stream-table join): job active state snapshot per event
CREATE STREAM app_dedup_with_job_state AS
SELECT
    a.job_id          AS job_id,
    j.status          AS job_status,
    j.is_deleted      AS job_is_deleted
FROM application_submitted_dedup a
LEFT JOIN job_state j ON a.job_id = j.post_id
EMIT CHANGES;

-- 5-minute tumbling window
-- NOTE on job_id_flat: ksqlDB does NOT write the GROUP BY key into the
-- VALUE of a windowed table (it lives only in the wrapped window key,
-- a ksqlDB-proprietary binary encoding unreadable by the JDBC sink's
-- primary.key extraction). LATEST_BY_OFFSET() re-projects the key INTO
-- the value so JdbcSinkConnector primary.key.mode=record_value can build
-- the (job_id, window_start, window_end) upsert key.
CREATE TABLE job_metrics_tumbling
  WITH (KAFKA_TOPIC = 'job_metrics_5m',
        VALUE_FORMAT = 'AVRO',
        PARTITIONS = 3) AS
SELECT
    job_id,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end,
    COUNT(*)    AS applications_cnt,
    SUM(CASE
            WHEN job_status = 'open' AND (job_is_deleted IS NULL OR job_is_deleted = 0)
            THEN 1 ELSE 0
        END) AS matches_cnt,
    LATEST_BY_OFFSET(job_id) AS job_id_flat
FROM app_dedup_with_job_state
WINDOW TUMBLING (SIZE 5 MINUTES, GRACE PERIOD 2 MINUTES)
GROUP BY job_id
EMIT CHANGES;

-- 10-minute hopping window advancing every 5 minutes (smoother trend)
CREATE TABLE job_metrics_hopping
  WITH (KAFKA_TOPIC = 'job_metrics_5m',
        VALUE_FORMAT = 'AVRO') AS
SELECT
    job_id,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end,
    COUNT(*)    AS applications_cnt,
    SUM(CASE
            WHEN job_status = 'open' AND (job_is_deleted IS NULL OR job_is_deleted = 0)
            THEN 1 ELSE 0
        END) AS matches_cnt,
    LATEST_BY_OFFSET(job_id) AS job_id_flat
FROM app_dedup_with_job_state
WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 5 MINUTES, GRACE PERIOD 2 MINUTES)
GROUP BY job_id
EMIT CHANGES;
