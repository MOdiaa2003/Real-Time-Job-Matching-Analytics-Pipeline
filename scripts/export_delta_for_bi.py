#!/usr/bin/env python3
"""Export every Delta Lake table under streaming_lake/ to Parquet + CSV for BI.

Output: exports/<table>.parquet (folder, single file part) and
exports/csv/<table>.csv (single file). Offline start (~40 s, pinned jars).
Run from repo root with the SYSTEM python (pyspark lives there):

    python scripts\\export_delta_for_bi.py
"""
import os
import shutil
import sys

import sys as _sys
try:
    if _sys.stdout.encoding and _sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
        _sys.stdout.reconfigure(encoding=_sys.stdout.encoding, errors="replace")
        _sys.stderr.reconfigure(encoding=_sys.stderr.encoding, errors="replace")
except (AttributeError, ValueError):
    pass

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_ROOT = os.path.join(REPO, "streaming_lake")
EXPORT = os.path.join(REPO, "exports")

from pyspark.sql import SparkSession

TABLES = ["candidate_dim", "job_dim", "pending_applications", "match_results", "match_invalidations", "dead_letter_applications"]


def build_spark():
    sys.path.insert(0, os.path.join(REPO, "Realtime_Pipline"))
    from match_engine import _local_package_jars  # reuse the pinned offline jars
    jar_list = _local_package_jars()
    return (SparkSession.builder
            .master("local[*]")
            .appName("delta-export-for-bi")
            .config("spark.jars", jar_list)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.parquet.compression.codec", "uncompressed")
            .getOrCreate())


def main():
    spark = build_spark()
    for t in TABLES:
        path = os.path.join(DELTA_ROOT, t)
        if not os.path.isdir(path):
            print(f"  skip {t} (no delta dir)")
            continue
        df = spark.read.format("delta").load(path)
        pq = os.path.join(EXPORT, f"{t}.parquet")
        if os.path.isdir(pq):
            shutil.rmtree(pq)
        df.coalesce(1).write.mode("overwrite").parquet(pq)
        csv = os.path.join(EXPORT, "csv")
        os.makedirs(csv, exist_ok=True)
        target = os.path.join(csv, t)
        if os.path.isdir(target):
            shutil.rmtree(target)
        # BI-friendly CSV: arrays -> comma text, structs/maps -> JSON text
        from pyspark.sql import functions as F
        from pyspark.sql.types import ArrayType, MapType, StructType
        flat = df
        for f_ in df.schema.fields:
            if isinstance(f_.dataType, ArrayType):
                flat = flat.withColumn(f_.name, F.concat_ws(", ", F.col(f_.name)))
            elif isinstance(f_.dataType, (MapType, StructType)):
                flat = flat.withColumn(f_.name, F.to_json(F.col(f_.name)))
        flat.coalesce(1).write.mode("overwrite").option("header", True).csv(target)
        n = df.count()
        print(f"  exported {t}: {n} rows")
    spark.stop()
    print("done -> exports\\*.parquet and exports\\csv\\*.csv")


if __name__ == "__main__":
    main()
