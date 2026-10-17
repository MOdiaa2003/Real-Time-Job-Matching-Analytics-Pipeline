"""
Gold — dim_company (Type 1, sequential keys).
================================================
Conformed company dimension. Column provenance:

  - company_key, company_name  : derived directly from silver data
  - industry                   : DERIVED, not guessed -- the most common `industry`
                                  value across that company's own job_posts rows
                                  (job_posts.industry is real data we already have).
                                  "Unknown" if the company never appears in job_posts
                                  (e.g. it's only someone's *previous* employer).
  - company_size, company_type, founded_year, headquarters_country,
    website_domain, employee_count, revenue_range
                                : joined from reference/company_reference_seed.csv, a
                                  STARTER seed with only a handful of extremely
                                  well-documented public companies (as a demo of the
                                  pattern). Everything else gets "Unknown"/NULL here
                                  and is_enriched=false -- we do not fabricate
                                  employee counts or founding years for companies we
                                  have no real source for.

Swap reference/company_reference_seed.csv for a real firmographic export
(Clearbit, Crunchbase, LinkedIn company API, your own CRM data, etc.),
keyed on company_name the same way, to get real coverage in production.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.config import SILVER_PATHS, GOLD_DIM_COMPANY, COMPANY_REFERENCE_SEED_CSV
from gold.surrogate_keys import next_start_number, assign_new_keys


def _distinct_company_names(spark: SparkSession):
    job = spark.read.format("delta").load(SILVER_PATHS["job"])
    from_jobs = job.select(F.col("company").alias("company_name"))

    candidate = spark.read.format("delta").load(SILVER_PATHS["candidate"])
    from_current = candidate.select(F.col("current_company").alias("company_name"))
    from_previous = candidate.select(F.explode_outer("previous_companies").alias("company_name"))

    return (
        from_jobs.unionByName(from_current).unionByName(from_previous)
        .withColumn("company_name", F.trim(F.col("company_name")))
        .filter(F.col("company_name").isNotNull() & (F.length("company_name") > 0))
        .distinct()
    )


def _derived_industry(spark: SparkSession):
    """Most frequent job_posts.industry per company -- real data, not a guess."""
    job = spark.read.format("delta").load(SILVER_PATHS["job"])
    counts = (
        job.groupBy("company", "industry")
        .agg(F.count("*").alias("n"))
        .filter(F.col("company").isNotNull() & F.col("industry").isNotNull())
    )
    w = Window.partitionBy("company").orderBy(F.col("n").desc())
    return (
        counts.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(F.col("company").alias("company_name"), F.col("industry"))
    )


def _enrich(spark: SparkSession, companies):
    industry = _derived_industry(spark)
    reference = spark.read.option("header", "true").csv(COMPANY_REFERENCE_SEED_CSV)

    enriched = (
        companies
        .join(industry, "company_name", "left")
        .withColumn("industry", F.coalesce(F.col("industry"), F.lit("Unknown")))
        .join(reference, "company_name", "left")
        .withColumn("is_enriched", F.col("company_size").isNotNull())
        .withColumn("company_size", F.coalesce(F.col("company_size"), F.lit("Unknown")))
        .withColumn("company_type", F.coalesce(F.col("company_type"), F.lit("Unknown")))
        .withColumn("founded_year", F.col("founded_year").cast("int"))
        .withColumn("headquarters_country", F.coalesce(F.col("headquarters_country"), F.lit("Unknown")))
        .withColumn("website_domain", F.coalesce(F.col("website_domain"), F.lit(None).cast("string")))
        .withColumn("employee_count", F.col("employee_count").cast("long"))
        .withColumn("revenue_range", F.coalesce(F.col("revenue_range"), F.lit("Unknown")))
    )
    return enriched


ATTRIBUTE_COLS = [
    "company_name", "industry", "company_size", "company_type", "founded_year",
    "headquarters_country", "website_domain", "employee_count", "revenue_range", "is_enriched",
]


def build_dim_company(spark: SparkSession):
    from delta.tables import DeltaTable

    companies = _distinct_company_names(spark)
    if companies.isEmpty():
        return
    enriched = _enrich(spark, companies)

    if DeltaTable.isDeltaTable(spark, GOLD_DIM_COMPANY):
        existing_keys = (
            spark.read.format("delta").load(GOLD_DIM_COMPANY)
            .select("company_name", "company_key")
        )
        with_keys = enriched.join(existing_keys, "company_name", "left")
        already_keyed = with_keys.filter(F.col("company_key").isNotNull())
        brand_new = with_keys.filter(F.col("company_key").isNull()).drop("company_key")

        start = next_start_number(spark, GOLD_DIM_COMPANY, "company_key")
        newly_keyed = assign_new_keys(
            brand_new, "company_key", "COMP", start, order_by="company_name",
        )
        staged = already_keyed.unionByName(newly_keyed).select("company_key", *ATTRIBUTE_COLS)

        dim = DeltaTable.forPath(spark, GOLD_DIM_COMPANY)
        (
            dim.alias("d")
            .merge(staged.alias("s"), "d.company_key = s.company_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        staged = assign_new_keys(enriched, "company_key", "COMP", 1, order_by="company_name")
        staged.select("company_key", *ATTRIBUTE_COLS).write.format("delta").mode("overwrite").save(GOLD_DIM_COMPANY)
