-- ─────────────────────────────────────────────────────────────────────
-- init_source.sql - Source PostgreSQL for CDC (Debezium)
-- Mirrors ensure_schema()/create_base_tables() in
-- Realtime_Pipline/recruitment_lifecycle_generator.py exactly; kept
-- here so the database is consistent even from a clean `reset`.
-- Idempotent: safe to re-run.
-- ─────────────────────────────────────────────────────────────────────

-- wal_level=logical is set in the container command (docker-compose.yml)

CREATE TABLE IF NOT EXISTS job_posts (
    post_id VARCHAR(50) PRIMARY KEY,
    company TEXT,
    category TEXT,
    job_title TEXT,
    application_method TEXT,
    post_date TIMESTAMP,
    deadline TIMESTAMP,
    certifications TEXT[],
    skills_needed TEXT[],
    platform TEXT,
    employment_type TEXT,
    city TEXT,
    country TEXT,
    language TEXT,
    work_mode TEXT,
    industry TEXT,
    detailed_experience_level TEXT,
    salary INT,
    years_experience TEXT,
    status VARCHAR(20) DEFAULT 'open',
    lifecycle_stage INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS candidate_info (
    candidate_id VARCHAR(50) PRIMARY KEY,
    name TEXT,
    gender TEXT,
    date_of_birth INT,
    nationality TEXT,
    location TEXT,
    graduation_year INT,
    GPA FLOAT,
    education_level TEXT,
    job_category TEXT,
    current_title TEXT,
    seniority TEXT,
    experience_range TEXT,
    current_company TEXT,
    previous_companies TEXT[],
    previous_industries TEXT[],
    skills TEXT[],
    certifications TEXT[],
    current_salary INT,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS applications (
    application_id VARCHAR(50) PRIMARY KEY,
    candidate_id VARCHAR(50),
    post_id VARCHAR(50),
    status VARCHAR(30),
    lifecycle_stage INT DEFAULT 0,
    applied_date TIMESTAMP,
    last_updated TIMESTAMP,
    final_outcome VARCHAR(30),
    FOREIGN KEY (candidate_id) REFERENCES candidate_info(candidate_id),
    FOREIGN KEY (post_id) REFERENCES job_posts(post_id)
);

-- ── Debezium requirements (idempotent) ────────────────────────────────
-- CDC audit columns: generator ensures these; make them present regardless.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'candidate_info' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE candidate_info
            ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN version INT DEFAULT 1;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'job_posts' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE job_posts
            ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN version INT DEFAULT 1;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'applications' AND column_name = 'created_at'
    ) THEN
        ALTER TABLE applications
            ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN version INT DEFAULT 1;
    END IF;
END $$;

-- Full row images so DELETE events carry the whole `before` payload
ALTER TABLE candidate_info REPLICA IDENTITY FULL;
ALTER TABLE job_posts       REPLICA IDENTITY FULL;
ALTER TABLE applications    REPLICA IDENTITY FULL;

-- ── Kafka replication user needs replication privilege already ────────
-- (postgres superuser is fine for the demo). Publication is handled by
-- the connector with publication.autocreate.mode = filtered.
