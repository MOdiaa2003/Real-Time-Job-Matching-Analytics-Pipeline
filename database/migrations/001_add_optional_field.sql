-- ─────────────────────────────────────────────────────────────────────
-- Migration 001 - schema-evolution demo (optional field)
-- Adds an optional column `remote_policy` to job_posts.
--
-- Demo contract: consumers must KEEP WORKING.
--   * JSON CDC path (Spark match_engine, Bronze/Silver): an unknown
--     column simply appears inside the `after` payload. Spark parses
--     with an explicit schema, so the new field is dropped silently -
--     consumers run unchanged.
--   * Avro path (Schema Registry, BACKWARD): re-registering the newest
--     schema with `remote_policy` as an optional/nullable field is
--     accepted (consumer-expected fields still exist, writer schema is
--     compatible); a BREAKING candidate schema is rejected by the
--     registry - shown in tests/03_schema_evolution.
-- Run against the SOURCE database:
--   docker exec -i my_postgres psql -U postgres -d recruitment < database/migrations/001_add_optional_field.sql
-- ─────────────────────────────────────────────────────────────────────
ALTER TABLE job_posts
    ADD COLUMN IF NOT EXISTS remote_policy VARCHAR(20);
