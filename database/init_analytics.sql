-- ─────────────────────────────────────────────────────────────────────
-- init_analytics.sql - Analytics PostgreSQL (JDBC sink target)
-- Holds live recruiter-facing aggregates + alerting tables.
-- ─────────────────────────────────────────────────────────────────────

-- 5-minute windowed job/application aggregates from ksqlDB
CREATE TABLE IF NOT EXISTS job_metrics_5m (
    job_id            VARCHAR(50)  NOT NULL,
    window_start      TIMESTAMP    NOT NULL,
    window_end        TIMESTAMP    NOT NULL,
    applications_cnt  BIGINT       DEFAULT 0,
    matches_cnt       BIGINT       DEFAULT 0,
    updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, window_start, window_end)
);

-- Spam/anomaly detection output
CREATE TABLE IF NOT EXISTS application_alerts (
    alert_id          SERIAL,
    candidate_id      VARCHAR(50)  NOT NULL,
    window_start      TIMESTAMP    NOT NULL,
    window_end        TIMESTAMP    NOT NULL,
    applications_cnt  BIGINT       NOT NULL,
    alert_reason      VARCHAR(100),
    updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (candidate_id, window_start, window_end)
);

-- Current job status snapshot for dashboards/recruiter UIs
CREATE TABLE IF NOT EXISTS job_current_state (
    job_id         VARCHAR(50) PRIMARY KEY,
    status         VARCHAR(20),
    work_mode      VARCHAR(20),
    city           TEXT,
    country        TEXT,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Matches invalidated by CLOSED/DELETED jobs (hydration from match_invalidation_events
-- and/or the Spark invalidation consumer)
CREATE TABLE IF NOT EXISTS match_invalidations (
    job_id            VARCHAR(50) NOT NULL,
    invalidated_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason            VARCHAR(50),
    prior_status      VARCHAR(20),
    PRIMARY KEY (job_id)
);
