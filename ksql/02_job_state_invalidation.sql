-- ═══════════════════════════════════════════════════════════════════════
-- 02_job_state_invalidation.sql
-- Business problem 1+2 (stale/closed recommendations must die fast):
--   * job_state = KTABLE of CURRENT job state (endpoint for lookups)
--   * closed/deleted transitions -> match_invalidation_events (filter leg)
--   * applications landing on closed jobs -> invalidations (JOIN leg)
-- ═══════════════════════════════════════════════════════════════════════

-- 1. KTable of CURRENT job state (materialized, compacted topic)
CREATE TABLE job_state (
    post_id     VARCHAR PRIMARY KEY,
    status      VARCHAR,
    op          VARCHAR,
    ts_ms       BIGINT,
    is_deleted  INTEGER
) WITH (KAFKA_TOPIC = 'job_state_events',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'JSON');

-- 2. Transition leg: a job's status changing to closed/filled, or delete,
--    emits invalidation for all of that job's active recommendations.
CREATE STREAM match_invalidation_events
  WITH (KAFKA_TOPIC = 'match_invalidation_events',
        VALUE_FORMAT = 'JSON',
        PARTITIONS = 3) AS
SELECT
    post_id                        AS job_id,
    status                         AS prior_status,
    CASE
        WHEN is_deleted = 1            THEN 'job_removed'
        WHEN status = 'filled'         THEN 'job_filled'
        ELSE 'job_closed'
    END                            AS reason,
    ts_ms                          AS invalidation_time_ms,
    op                             AS source_op
FROM job_state_events
WHERE (op = 'd' OR is_deleted = 1 OR status = 'closed' OR status = 'filled')
EMIT CHANGES;

-- 3. Prevention leg (stream-table JOIN): applications arriving on jobs
--    that are ALREADY closed get flagged immediately too.
CREATE STREAM applications_by_job AS
SELECT
    post_id          AS join_job_id,
    application_id,
    candidate_id,
    status,
    op,
    ts_ms
FROM application_state_events
PARTITION BY post_id
EMIT CHANGES;

CREATE STREAM match_invalidation_events_app_leg
  WITH (KAFKA_TOPIC = 'match_invalidation_events',
        VALUE_FORMAT = 'JSON') AS
SELECT
    a.join_job_id                            AS job_id,
    CAST(NULL AS VARCHAR)                    AS prior_status,
    'job_closed'                             AS reason,
    a.ts_ms                                  AS invalidation_time_ms,
    a.op                                     AS source_op
FROM applications_by_job a
JOIN job_state js ON a.join_job_id = js.post_id
WHERE js.status IN ('closed', 'filled') OR js.is_deleted = 1
EMIT CHANGES;
