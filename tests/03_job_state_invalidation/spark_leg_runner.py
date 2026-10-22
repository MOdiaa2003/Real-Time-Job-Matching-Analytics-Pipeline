"""Suites/03 Spark leg runner - MUST run under the SYSTEM python
(pyspark 3.5.3 + delta-spark 3.2.0 live there, not in .venv-streams).

1. spawn the match engine
2. build ONE Spark session and poll reading
   streaming_lake/match_invalidations until the target job_id appears
3. print HIT ... or FAIL ...; exit 0/1; ensure the engine is terminated.

Usage: python spark_leg_runner.py <job_id>
"""
import os
import pathlib
import subprocess
import sys
import time

REPO = pathlib.Path(__file__).resolve().parents[2]


def main():
    job_id = sys.argv[1]
    deltadir = REPO / "streaming_lake" / "match_invalidations"
    eng = subprocess.Popen(
        ["python", str(REPO / "Realtime_Pipline" / "match_engine.py")],
        cwd=str(REPO), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        from pyspark.sql import SparkSession
        builder = (SparkSession.builder
                   .appName("test03-read-invalidations")
                   .config("spark.jars.packages",
                           "io.delta:delta-spark_2.12:3.2.0,"
                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
                   .config("spark.sql.extensions",
                           "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog",
                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   .config("spark.sql.parquet.compression.codec", "uncompressed")
                   .config("spark.ui.enabled", "false"))
        # NB: delta-spark pip does NOT put its jars on the classpath by
        # itself. The spark.jars.packages config above (same as the engine's
        # build_spark) is what makes reads work; without it every read
        # failed DATA_SOURCE_NOT_FOUND and the old except-pass made it look
        # like "table never populated".
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        import delta.tables  # noqa: F401  (registers deltaformat parser)

        deadline = time.time() + 700  # cold starts (ivy/maven) can take minutes
        attempts = 0
        last_err = None
        while time.time() < deadline:
            attempts += 1
            rows = []
            try:
                rows = spark.read.format("delta").load(str(deltadir)).collect()
            except Exception as e:  # table not committed yet -> keep waiting
                last_err = e
            for r in rows:
                if r["job_id"] == job_id:
                    print(f"HIT job_id={r['job_id']} reason={r['reason']} "
                          f"prior_status={r['prior_status']} "
                          f"source_op={r['source_op']} "
                          f"(attempts={attempts}, total_rows={len(rows)})")
                    return 0
            if eng.poll() is not None:
                print(f"FAIL match engine exited early rc={eng.returncode}")
                return 1
            print(f"  waiting... attempt {attempts} rows={len(rows)} "
                  f"engine_alive={eng.poll() is None} "
                  f"last_read_error={repr(last_err)[:90] if last_err else '-'}",
                  flush=True)
            time.sleep(15)
        print(f"FAIL Delta match_invalidations did not receive {job_id} "
              f"within timeout")
        return 1
    finally:
        eng.terminate()
        try:
            eng.wait(10)
        except Exception:
            eng.kill()


if __name__ == "__main__":
    sys.exit(main())
