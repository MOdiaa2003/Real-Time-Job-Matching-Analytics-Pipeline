"""
Gold — fact_application.
==========================
Per the data-model decision: applications are a lifecycle/event fact, not
a dimension. Every silver application event (applied -> viewed ->
interview -> offer -> {rejected|accepted}) becomes its own fact row --
we do NOT collapse to "current status only," so funnel/stage-duration
analysis stays possible in the warehouse.

Each fact row is linked to the dimension row that was CURRENT-AS-OF the
event's timestamp (a proper as-of join), not just today's current row --
otherwise historical facts would silently drift to reflect today's
candidate/job attributes. The dimension row is addressed by its natural
key + valid_from, which together are unique in an SCD2 table and serve as
the composite surrogate key referenced by the fact.

company_key / location_key: dim_company and dim_location now use stable
SEQUENTIAL keys (LOC_000125, COMP_000047), not a hash -- so unlike before,
they genuinely have to be looked up via a real join rather than
recomputed. We take dim_job's as-of company/city/country (already tracked
in dim_job.TRACKED_COLS) and join those to dim_company/dim_location to
resolve the two extra foreign keys.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import (
    SILVER_PATHS, GOLD_DIM_CANDIDATE, GOLD_DIM_JOB, GOLD_DIM_COMPANY,
    GOLD_DIM_LOCATION, GOLD_FACT_APPLICATION,
)
from gold.dim_location import normalize_country_col


def _as_of_join(fact, dim, natural_key, event_ts_col, prefix, extra_cols=None):
    extra_cols = extra_cols or []
    dim_slim = dim.select(
        F.col(natural_key).alias(f"_{prefix}_key"),
        F.col("valid_from").alias(f"_{prefix}_valid_from"),
        F.col("valid_to").alias(f"_{prefix}_valid_to"),
        *[F.col(c).alias(f"_{prefix}_{c}") for c in extra_cols],
    )
    joined = fact.join(
        dim_slim,
        (fact[natural_key] == dim_slim[f"_{prefix}_key"]) &
        (dim_slim[f"_{prefix}_valid_from"] <= fact[event_ts_col]) &
        (dim_slim[f"_{prefix}_valid_to"].isNull() | (dim_slim[f"_{prefix}_valid_to"] > fact[event_ts_col])),
        how="left",
    )
    joined = joined.withColumnRenamed(f"_{prefix}_valid_from", f"{prefix}_dim_valid_from") \
                    .drop(f"_{prefix}_key", f"_{prefix}_valid_to")
    for c in extra_cols:
        joined = joined.withColumnRenamed(f"_{prefix}_{c}", f"{prefix}_{c}")
    return joined


def build_fact_application(spark: SparkSession, since_date: str = None):
    silver = spark.read.format("delta").load(SILVER_PATHS["application"])
    if since_date:
        silver = silver.filter(silver["_ingested_at"] >= since_date)
    if silver.isEmpty():  # native check, no .rdd conversion -> no Python worker involved
        return

    dim_candidate = spark.read.format("delta").load(GOLD_DIM_CANDIDATE)
    dim_job = spark.read.format("delta").load(GOLD_DIM_JOB)
    dim_company = spark.read.format("delta").load(GOLD_DIM_COMPANY).select("company_name", "company_key")
    dim_location = spark.read.format("delta").load(GOLD_DIM_LOCATION).select(
        "city_norm", "country", F.col("location_key")
    )

    fact = silver.select(
        "application_id", "candidate_id", "post_id", "status", "lifecycle_stage",
        "applied_date", "last_updated", "final_outcome", "op", "version",
        "source_ts_ms", "is_deleted", "_event_id",
    )

    fact = _as_of_join(fact, dim_candidate, "candidate_id", "source_ts_ms", "candidate")
    fact = _as_of_join(
        fact, dim_job, "post_id", "source_ts_ms", "job",
        extra_cols=["company", "city", "country_norm"],
    )

    # resolve company_key via a real lookup (sequential keys can't be recomputed)
    fact = (
        fact.join(
            dim_company,
            F.trim(fact["job_company"]) == dim_company["company_name"],
            "left",
        )
        .drop("company_name")
    )

    # resolve location_key the same way, matching dim_location's own key shape
    fact = (
        fact.withColumn("_job_city_norm", F.lower(F.trim(F.col("job_city"))))
        .withColumn("_job_country_norm", normalize_country_col(F.col("job_country_norm")))
        .join(
            dim_location,
            (F.col("_job_city_norm") == dim_location["city_norm"]) &
            (F.col("_job_country_norm") == dim_location["country"]),
            "left",
        )
        .drop("_job_city_norm", "_job_country_norm", "city_norm", "country")
    )

    fact = fact.drop("job_company", "job_city", "job_country_norm")

    (
        fact.write.format("delta").mode("append")
        .partitionBy("status")
        .option("mergeSchema", "true")
        .save(GOLD_FACT_APPLICATION)
    )
