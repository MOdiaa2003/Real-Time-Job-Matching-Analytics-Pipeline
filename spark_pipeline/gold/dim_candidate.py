"""Gold — dim_candidate (SCD2). Tracks slowly-changing candidate attributes."""

from pyspark.sql import SparkSession

from common.config import SILVER_PATHS, GOLD_DIM_CANDIDATE
from gold.scd2 import apply_scd2

TRACKED_COLS = [
    "name", "gender", "location", "country_norm", "current_title", "seniority",
    "current_company", "job_category", "education_level",
    "experience_range", "experience_min_years", "experience_max_years",
    "skills_norm", "certifications_norm", "current_salary_norm", "is_active",
]


def build_dim_candidate(spark: SparkSession, since_date: str = None):
    silver = spark.read.format("delta").load(SILVER_PATHS["candidate"])
    if since_date:
        silver = silver.filter(silver["_ingested_at"] >= since_date)
    apply_scd2(
        spark, silver, GOLD_DIM_CANDIDATE,
        natural_key="candidate_id", tracked_cols=TRACKED_COLS,
    )
