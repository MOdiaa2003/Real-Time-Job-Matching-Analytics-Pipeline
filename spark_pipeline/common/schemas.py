"""
Explicit schemas for CDC payloads.
==================================
Debezium's JsonConverter (schemas.enable=false, see debezuim_connector.py)
emits schema-less JSON, so Spark can't infer types safely at scale
(infer-on-sample will get bitten by nulls/arrays). We declare the row shape
of each Postgres table plus the Debezium envelope that wraps it, matching
recruitment_lifecycle_generator.py's CREATE TABLE statements exactly.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType,
    BooleanType, ArrayType, LongType
)

# ── candidate_info row (before/after payload on the candidate_events topic) ──
CANDIDATE_ROW_SCHEMA = StructType([
    StructField("candidate_id", StringType()),
    StructField("name", StringType()),
    StructField("gender", StringType()),
    StructField("date_of_birth", IntegerType()),
    StructField("nationality", StringType()),
    StructField("location", StringType()),
    StructField("graduation_year", IntegerType()),
    StructField("gpa", FloatType()),
    StructField("education_level", StringType()),
    StructField("job_category", StringType()),
    StructField("current_title", StringType()),
    StructField("seniority", StringType()),
    StructField("experience_range", StringType()),
    StructField("current_company", StringType()),
    StructField("previous_companies", ArrayType(StringType())),
    StructField("previous_industries", ArrayType(StringType())),
    StructField("skills", ArrayType(StringType())),
    StructField("certifications", ArrayType(StringType())),
    StructField("current_salary", IntegerType()),
    StructField("is_active", BooleanType()),
    # Debezium ships Postgres TIMESTAMP as epoch micros (string in the
    # sample payload) -- keep as string in bronze, cast in silver.
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("version", IntegerType()),
])

# ── job_posts row (job_events topic) ──────────────────────────────────────
JOB_ROW_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("company", StringType()),
    StructField("category", StringType()),
    StructField("job_title", StringType()),
    StructField("application_method", StringType()),
    StructField("post_date", StringType()),
    StructField("deadline", StringType()),
    StructField("certifications", ArrayType(StringType())),
    StructField("skills_needed", ArrayType(StringType())),
    StructField("platform", StringType()),
    StructField("employment_type", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("language", StringType()),
    StructField("work_mode", StringType()),
    StructField("industry", StringType()),
    StructField("detailed_experience_level", StringType()),
    StructField("salary", IntegerType()),
    StructField("years_experience", StringType()),
    StructField("status", StringType()),
    StructField("lifecycle_stage", IntegerType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("version", IntegerType()),
])

# ── applications row (application_events topic) ──────────────────────────
APPLICATION_ROW_SCHEMA = StructType([
    StructField("application_id", StringType()),
    StructField("candidate_id", StringType()),
    StructField("post_id", StringType()),
    StructField("status", StringType()),
    StructField("lifecycle_stage", IntegerType()),
    StructField("applied_date", StringType()),
    StructField("last_updated", StringType()),
    StructField("final_outcome", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("version", IntegerType()),
])

ROW_SCHEMAS = {
    "candidate": CANDIDATE_ROW_SCHEMA,
    "job": JOB_ROW_SCHEMA,
    "application": APPLICATION_ROW_SCHEMA,
}

_SOURCE_SCHEMA = StructType([
    StructField("version", StringType()),
    StructField("connector", StringType()),
    StructField("name", StringType()),
    StructField("ts_ms", LongType()),
    StructField("snapshot", StringType()),
    StructField("db", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
    StructField("txId", LongType()),
    StructField("lsn", LongType()),
])


def envelope_schema(row_schema: StructType) -> StructType:
    """Full Debezium envelope: {before, after, source, op, ts_ms, ...}."""
    return StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("source", _SOURCE_SCHEMA),
        StructField("op", StringType()),       # c=create, u=update, d=delete, r=snapshot-read
        StructField("ts_ms", LongType()),
    ])


ENVELOPE_SCHEMAS = {e: envelope_schema(s) for e, s in ROW_SCHEMAS.items()}
