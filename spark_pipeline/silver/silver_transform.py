"""
Silver layer — cleaned, deduplicated, flattened CDC events.
=============================================================
One silver row = one validated, normalized CDC event (still history, not
yet collapsed into a dimension -- that's gold's job). Grain: one row per
(entity_id, version, op).

Steps, in order:
  1. dedupe on the true dedup key: entity_id + version + op (a Kafka
     replay of the same offset would otherwise double-count a change).
  2. flatten: op in (c,u,r) -> use `after`; op == 'd' -> use `before`
     (Debezium sets `after` to null on delete) and stamp is_deleted=true.
  3. CDC gap detection: within each entity_id, version must increase by
     exactly 1 run-over-run. A skip (e.g. v3 -> v5) means a WAL event was
     lost or arrived out of order -- flagged, not dropped, since the row
     is still the latest known state.
  4. normalize (silver/normalization.py).
  5. validate + quarantine (silver/validation.py).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.config import BRONZE_PATHS, SILVER_PATHS
from silver.normalization import add_candidate_normalization, add_job_normalization
from silver.validation import validate_and_split

ID_COL = {"candidate": "candidate_id", "job": "post_id", "application": "application_id"}


def _flatten(df: DataFrame, entity: str) -> DataFrame:
    id_col = ID_COL[entity]
    payload = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    flat = df.withColumn("_payload", payload).select(
        "_payload.*",
        "op",
        F.col("ts_ms").alias("source_ts_ms"),
        F.col("_event_id"),
        F.col("_ingested_at"),
        F.col("_kafka_offset"),
    )
    return flat.withColumn("is_deleted", F.col("op") == "d") \
               .filter(F.col(id_col).isNotNull())


def _dedupe(df: DataFrame, entity: str) -> DataFrame:
    id_col = ID_COL[entity]
    w = Window.partitionBy(id_col, "version", "op").orderBy(F.col("source_ts_ms").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def _flag_version_gaps(df: DataFrame, entity: str) -> DataFrame:
    id_col = ID_COL[entity]
    w = Window.partitionBy(id_col).orderBy("version")
    return df.withColumn("_prev_version", F.lag("version").over(w)) \
             .withColumn(
                 "has_version_gap",
                 F.col("_prev_version").isNotNull() & (F.col("version") > F.col("_prev_version") + 1),
             ) \
             .drop("_prev_version")


def transform_entity(spark: SparkSession, entity: str, since_date: str = None) -> int:
    bronze = spark.read.format("delta").load(BRONZE_PATHS[entity])
    if since_date:
        bronze = bronze.filter(F.col("_ingest_date") >= since_date)
    if bronze.isEmpty():  # native check, no .rdd conversion -> no Python worker involved
        return 0

    flat = _flatten(bronze, entity)
    deduped = _dedupe(flat, entity)
    gap_flagged = _flag_version_gaps(deduped, entity)

    if entity == "candidate":
        normalized = add_candidate_normalization(gap_flagged)
    elif entity == "job":
        normalized = add_job_normalization(gap_flagged)
    else:
        normalized = gap_flagged  # applications: lifecycle status, little to normalize

    # CACHE HERE: `normalized` carries two window-function shuffles
    # (_dedupe, _flag_version_gaps). Without caching, validate_and_split's
    # internal quarantine write, the silver write below, and the final
    # count() would each redo the flatten+dedupe+gap-check+normalize chain
    # from scratch -- 3x the shuffles for no reason.
    normalized = normalized.cache()

    valid = validate_and_split(spark, normalized, entity).cache()

    (
        valid.write.format("delta").mode("append")
        .partitionBy("op")
        .option("mergeSchema", "true")
        .save(SILVER_PATHS[entity])
    )
    row_count = valid.count()
    normalized.unpersist()
    valid.unpersist()
    return row_count


def run_silver(spark: SparkSession) -> dict:
    return {entity: transform_entity(spark, entity) for entity in ID_COL}
