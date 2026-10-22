-- ═══════════════════════════════════════════════════════════════════════
-- 01_flat_source_topics.sql - SOURCE STREAM declarations
-- (flat projections written by the Quix stream-processor; see README for
--  why ksqlDB never parses Debezium envelopes directly)
-- ═══════════════════════════════════════════════════════════════════════
SET 'auto.offset.reset' = 'earliest';

-- Web-App dedup output (Avro, schema registered by the Quix processor).
-- Declared WITHOUT explicit columns: ksqlDB pulls the writer schema from
-- the Schema Registry (subject application_submitted_dedup-value).
CREATE STREAM application_submitted_dedup
  WITH (KAFKA_TOPIC = 'application_submitted_dedup',
        VALUE_FORMAT = 'AVRO',
        KEY_FORMAT = 'KAFKA',
        TIMESTAMP = 'event_time_ms');

-- Flat CDC projection of job_posts (key = post_id, JSON)
CREATE STREAM job_state_events (
    post_id      VARCHAR,
    status       VARCHAR,
    job_status   VARCHAR,
    op           VARCHAR,
    ts_ms        BIGINT,
    remote_policy VARCHAR,
    is_deleted   INTEGER
) WITH (KAFKA_TOPIC = 'job_state_events',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'JSON');

-- Flat CDC projection of applications (key = application_id, JSON)
CREATE STREAM application_state_events (
    application_id  VARCHAR,
    candidate_id    VARCHAR,
    post_id         VARCHAR,
    status          VARCHAR,
    lifecycle_stage INTEGER,
    final_outcome   VARCHAR,
    op              VARCHAR,
    ts_ms           BIGINT,
    is_deleted      INTEGER
) WITH (KAFKA_TOPIC = 'application_state_events',
        KEY_FORMAT = 'KAFKA',
        VALUE_FORMAT = 'JSON');
