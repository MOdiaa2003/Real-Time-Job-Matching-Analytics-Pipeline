#!/usr/bin/env python3
"""
Batch medallion pipeline entry point.
======================================
Kafka (candidate_events / job_events / application_events)
        │
        ▼
   BRONZE  (raw envelopes, append-only, lineage columns)
        │
        ▼
   SILVER  (flattened, deduped, gap-checked, normalized, validated;
            rejects -> quarantine)
        │
        ▼
   GOLD    (dim_candidate SCD2, dim_job SCD2, dim_company, dim_location,
            dim_date, fact_application) -> data warehouse consumption layer

Run directly for a local/dev pass:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        orchestration/run_batch_pipeline.py

In prod this is one Airflow task per stage (bronze -> silver -> gold) so a
silver bug doesn't force re-reading Kafka, and a gold bug doesn't force
re-running silver. Wire each `run_*` function below to its own task.
"""

import sys
import logging

from pyspark.sql import SparkSession

sys.path.append(".")  # allow `common.*`, `bronze.*` etc. imports when run via spark-submit

from bronze.bronze_ingest import run_bronze
from silver.silver_transform import run_silver
from gold.dim_candidate import build_dim_candidate
from gold.dim_job import build_dim_job
from gold.dim_company import build_dim_company
from gold.dim_location import build_dim_location
from gold.dim_date import build_dim_date
from gold.fact_application import build_fact_application

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("batch_pipeline")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("recruitment-medallion-batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Spark's default of 200 shuffle partitions is tuned for cluster-scale
        # data. On a local run over ~100K-1M rows it just means hundreds of
        # tiny tasks and scheduling overhead for every window/join in silver
        # and gold. 8 is a much better fit for local[*] on data this size --
        # raise it back toward 200 if you point this at a real cluster.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    spark = get_spark()

    log.info("── BRONZE ─────────────────────────────")
    bronze_counts = run_bronze(spark)
    log.info("bronze rows ingested: %s", bronze_counts)

    log.info("── SILVER ─────────────────────────────")
    silver_counts = run_silver(spark)
    log.info("silver rows written: %s", silver_counts)

    log.info("── GOLD ───────────────────────────────")
    build_dim_candidate(spark)
    build_dim_job(spark)
    build_dim_company(spark)         # sourced from job + candidate silver, no dependency on dim_job/dim_candidate
    build_dim_location(spark)        # same
    build_dim_date(spark)            # static calendar, no dependency
    build_fact_application(spark)    # depends on dim_candidate + dim_job (as-of join, incl. company/location keys)
    log.info("gold dimensions/fact refreshed")

    spark.stop()


if __name__ == "__main__":
    main()
