"""
Gold — sequential surrogate keys.
====================================
Produces keys like 'LOC_000125' / 'COMP_000047' instead of a hash. Unlike
a hash (which is stable "for free"), sequential numbering needs explicit
bookkeeping: existing dimension rows must keep the exact key they already
have, and only genuinely new rows get the next number(s) in the sequence
-- otherwise every rerun would renumber the whole table and break every
foreign key pointing at it.

Usage pattern for a Type 1 dimension (see gold/dim_company.py,
gold/dim_location.py):
  1. Build `all_rows` = every distinct entity currently in silver.
  2. Left-join `all_rows` to the existing dim on the natural key to see
     which ones already have a key.
  3. Rows with no existing key go through `assign_new_keys()` here.
  4. MERGE the (existing-key rows + newly-keyed rows) into the dim table:
     update attributes on match, insert on no-match.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def next_start_number(spark: SparkSession, dim_path: str, key_col: str) -> int:
    """Highest existing sequence number in the dim + 1, or 1 if the dim doesn't exist yet."""
    if not DeltaTable.isDeltaTable(spark, dim_path):
        return 1
    existing_max = (
        spark.read.format("delta").load(dim_path)
        .select(F.regexp_extract(F.col(key_col), r"_(\d+)$", 1).cast("long").alias("n"))
        .agg(F.max("n").alias("max_n"))
        .collect()[0]["max_n"]
    )
    return (existing_max or 0) + 1


def assign_new_keys(
    new_rows: DataFrame, key_col: str, prefix: str, start: int, order_by: str, pad_width: int = 6,
) -> DataFrame:
    """
    Adds `key_col` to `new_rows`, numbering them start, start+1, start+2...
    in `order_by` order (deterministic -- reruns with the same new rows
    produce the same keys even before they're persisted).
    """
    w = Window.orderBy(order_by)
    numbered = new_rows.withColumn("_rn", F.row_number().over(w))
    return (
        numbered
        .withColumn(
            key_col,
            F.concat(F.lit(f"{prefix}_"), F.lpad((F.col("_rn") + F.lit(start - 1)).cast("string"), pad_width, "0")),
        )
        .drop("_rn")
    )
