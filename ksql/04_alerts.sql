-- =========================================================================
-- 04_alerts.sql
-- Business problem 6 (abnormal/spam activity - the banking "fraud alert"
-- concept adapted to recruitment): one candidate applying to more than
-- N jobs within a hopping window (N = ALERT_APPLY_LIMIT, default 5).
--
-- NOTE on COUNT: ksqlDB does not support COUNT(DISTINCT x) inside windowed
-- aggregations. COUNT(*) is used instead; since the event-id replays are
-- already removed by the Quix dedup processor, COUNT(*) is effectively the
-- number of DISTINCT (application_id) submissions in the window - document
-- this trade-off in the interview.
-- Output: application_alerts hooking the JDBC sink / dashboards.
-- EVENT time (event_time_ms); grace period lets stragglers still count.
-- =========================================================================
CREATE TABLE application_alerts
  WITH (KAFKA_TOPIC = 'application_alerts',
        VALUE_FORMAT = 'AVRO',
        PARTITIONS = 3) AS
SELECT
    candidate_id,
    WINDOWSTART AS window_start,
    WINDOWEND   AS window_end,
    COUNT(*) AS applications_cnt,
    LATEST_BY_OFFSET(candidate_id) AS candidate_id_flat,
    'apply_spam' AS alert_reason
FROM application_submitted_dedup
WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 5 MINUTES, GRACE PERIOD 1 MINUTE)
GROUP BY candidate_id
HAVING COUNT(*) > 5
EMIT CHANGES;
