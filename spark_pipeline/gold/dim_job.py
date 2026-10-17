"""Gold — dim_job (SCD2). Tracks slowly-changing job-post attributes."""

from pyspark.sql import SparkSession

from common.config import SILVER_PATHS, GOLD_DIM_JOB
from gold.scd2 import apply_scd2

TRACKED_COLS = [
    "company", "category", "job_title", "city", "country_norm", "work_mode",
    "industry", "detailed_experience_level", "employment_type",
    "salary_norm", "years_experience", "experience_min_years", "experience_max_years",
    "skills_needed_norm", "certifications_norm", "status", "lifecycle_stage",
]


def build_dim_job(spark: SparkSession, since_date: str = None):
    silver = spark.read.format("delta").load(SILVER_PATHS["job"])
    if since_date:
        silver = silver.filter(silver["_ingested_at"] >= since_date)
    apply_scd2(
        spark, silver, GOLD_DIM_JOB,
        natural_key="post_id", tracked_cols=TRACKED_COLS,
    )
