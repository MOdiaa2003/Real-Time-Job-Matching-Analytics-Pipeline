"""
Gold layer — generic SCD Type 2 processor.
=============================================
Deliberately generic: takes a natural key + a set of tracked attributes
and applies standard SCD2 semantics via a Delta MERGE. Both
gold/dim_candidate.py and gold/dim_job.py just call this with a different
column list -- and because it's expressed as "apply this batch of
already-deduped CDC rows," the same function is what you'd call from a
Spark Structured Streaming foreachBatch later; only the caller (batch vs.
streaming trigger) changes, not the SCD2 logic itself.

Semantics per natural_key:
  - no existing current row               -> insert new row, is_current=true
  - existing current row, attrs changed    -> close old row (valid_to,
                                               is_current=false), insert new
                                               current row
  - existing current row, attrs unchanged  -> no-op (avoids row bloat from
                                               events that don't touch
                                               tracked columns, e.g. an
                                               application-only update)
  - op == 'd' (source hard delete)         -> close the current row,
                                               nothing reopens it
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _latest_per_key(incremental: DataFrame, natural_key: str) -> DataFrame:
    """Collapse a batch to one row per key: the highest `version` wins."""
    w = Window.partitionBy(natural_key).orderBy(F.col("version").desc())
    return (
        incremental.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def apply_scd2(
    spark: SparkSession,
    incremental: DataFrame,
    dim_table_path: str,
    natural_key: str,
    tracked_cols: list,
    event_ts_col: str = "source_ts_ms",
):
    latest = _latest_per_key(incremental, natural_key)

    change_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("␀")) for c in tracked_cols]), 256)
    staged = latest.withColumn("_row_hash", change_hash) \
                    .withColumn("valid_from", F.col(event_ts_col)) \
                    .withColumn("valid_to", F.lit(None).cast("long")) \
                    .withColumn("is_current", ~F.col("is_deleted"))

    if not DeltaTable.isDeltaTable(spark, dim_table_path):
        (
            staged.select(natural_key, *tracked_cols, "version", "_row_hash",
                          "valid_from", "valid_to", "is_current")
            .write.format("delta").mode("overwrite").save(dim_table_path)
        )
        return

    dim = DeltaTable.forPath(spark, dim_table_path)

    # 1) close current rows whose tracked attributes actually changed, or
    #    that were hard-deleted at source.
    (
        dim.alias("d")
        .merge(
            staged.alias("s"),
            f"d.{natural_key} = s.{natural_key} AND d.is_current = true",
        )
        .whenMatchedUpdate(
            condition="d._row_hash <> s._row_hash",
            set={"is_current": F.lit(False), "valid_to": F.col(f"s.{event_ts_col}")},
        )
        .execute()
    )

    # 2) insert a fresh "current" row for any key that is new, or whose
    #    latest change hash isn't already represented as the current row
    #    (i.e. it was just closed in step 1, or never existed).
    current_hashes = dim.toDF().filter(F.col("is_current")).select(natural_key, "_row_hash")
    to_insert = (
        staged.join(current_hashes, on=natural_key, how="left_anti")
        .filter(~F.col("is_deleted"))
        .select(natural_key, *tracked_cols, "version", "_row_hash", "valid_from", "valid_to", "is_current")
    )
    to_insert.write.format("delta").mode("append").save(dim_table_path)
