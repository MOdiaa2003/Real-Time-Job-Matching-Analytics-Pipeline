# #!/usr/bin/env python3
# """
# Real-Time Candidate ↔ Job Matching Engine (Optimized)
# """

# import os
# import sys
# from dotenv import load_dotenv

# # Pin the worker Python to the exact interpreter running this script.
# # On Windows especially, if PYSPARK_PYTHON is left unset, the driver and
# # spawned Python workers can resolve to different `python.exe` installs
# # (e.g. a spark-submit bundled one vs. a pip/venv one), which can crash
# # workers with `EOFException` / "Python worker exited unexpectedly" even
# # before any user code (UDFs, etc.) runs. Setting this before Spark starts
# # removes that ambiguity. Must be set before SparkSession is created.
# os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
# os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# load_dotenv()

# from pyspark.sql import SparkSession, DataFrame, Window
# from pyspark.sql import functions as F
# from pyspark.sql.types import (
#     StructType, StructField, StringType, IntegerType, LongType,
#     FloatType, BooleanType, ArrayType, TimestampType
# )
# from delta.tables import DeltaTable

# KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "F:/DE_Projects/job_posting/project/checkpoint")
# DELTA_ROOT = os.getenv("DELTA_ROOT", "F:/DE_Projects/job_posting/project/streaming_lake")

# TOPICS = {
#     "candidate": "candidate_events",
#     "job": "job_events",
#     "application": "application_events",
# }

# MATCH_WEIGHTS = {
#     "skills": 0.30,
#     "experience": 0.25,
#     "salary": 0.15,
#     "location": 0.20,
#     "language": 0.10,
# }

# PRIORITY_THRESHOLDS = {
#     "HIGH": 85,
#     "MEDIUM": 65,
# }

# MAX_ENRICHMENT_RETRIES = 5

# COUNTRY_LANGUAGES = {
#     "united states": "english", "united kingdom": "english", "canada": "english",
#     "australia": "english", "india": "english", "germany": "german",
#     "france": "french", "spain": "spanish", "mexico": "spanish",
#     "brazil": "portuguese", "portugal": "portuguese", "italy": "italian",
#     "netherlands": "dutch", "sweden": "swedish", "poland": "polish",
#     "egypt": "arabic", "saudi arabia": "arabic", "uae": "arabic",
#     "japan": "japanese", "south korea": "korean", "china": "mandarin",
# }


# # Number of local worker threads/cores Spark will use. Defaults to 2.
# # On Windows, each Spark task that needs a Python process (even a trivial
# # write of an empty DataFrame) launches a brand-new python.exe subprocess -
# # there's no fork(), unlike Linux/Mac. Using local[*] (all cores) means
# # spawning that many processes essentially simultaneously, which is slow
# # and easy for antivirus/Defender to disrupt mid-launch, producing random
# # "Python worker exited unexpectedly (crashed)" / EOFException failures on
# # whichever task lost the race - not tied to any particular row or code
# # path. Lowering this reduces how many workers start at once. Override
# # with SPARK_LOCAL_CORES if you want to dial it back up once things are
# # stable.
# SPARK_LOCAL_CORES = os.getenv("SPARK_LOCAL_CORES", "2")

# # Every crash log so far has shown the SAME file failing to clean up
# # afterward: org.xerial.snappy_snappy-java-*.jar - the native Snappy
# # compression library, which Delta/Parquet writes use by default and which
# # gets extracted to a temp .dll at runtime. That's a very common target for
# # Windows Defender / AV real-time scanning to lock mid-extraction, which
# # would explain crashes that persist even at local[2] (i.e. not actually a
# # concurrency race - every write needs this same native lib).
# # Set SPARK_PARQUET_CODEC=uncompressed (or "lz4", which has its own native
# # lib but is a useful second data point) to test this without editing code.
# SPARK_PARQUET_CODEC = os.getenv("SPARK_PARQUET_CODEC", "snappy")


# # def build_spark() -> SparkSession:
# #     return (
# #         SparkSession.builder
# #         .appName("candidate-job-match-engine")
# #         .master(f"local[{SPARK_LOCAL_CORES}]")
# #         .config("spark.jars.packages",
# #                 "io.delta:delta-spark_2.12:3.2.0,"
# #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
# #         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# #         .config("spark.sql.catalog.spark_catalog",
# #                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
# #         .config("spark.sql.shuffle.partitions", "8")
# #         .config("spark.sql.parquet.compression.codec", SPARK_PARQUET_CODEC)
# #         # Reuse worker processes across tasks instead of spawning a fresh
# #         # one per task where possible - fewer process launches, fewer
# #         # chances for one to get killed mid-startup.
# #         .config("spark.python.worker.reuse", "true")
# #         # If a task's worker still gets killed transiently (e.g. AV), retry
# #         # instead of failing the whole job outright.
# #         .config("spark.task.maxFailures", "4")
# #         .getOrCreate()
# #     )
# def build_spark() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("candidate-job-match-engine")
#         .master(f"local[{SPARK_LOCAL_CORES}]")
#         .config("spark.jars.packages",
#                 "io.delta:delta-spark_2.12:3.2.0,"
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog",
#                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#         .config("spark.sql.shuffle.partitions", "8")
#         # FIXED: bypass Snappy native DLL entirely on Windows
#         .config("spark.sql.parquet.compression.codec", "uncompressed")
#         .config("spark.python.worker.reuse", "true")
#         .config("spark.task.maxFailures", "4")
#         .getOrCreate()
#     )

# CANDIDATE_AFTER_SCHEMA = StructType([
#     StructField("candidate_id", StringType()),
#     StructField("name", StringType()),
#     StructField("gender", StringType()),
#     StructField("date_of_birth", IntegerType()),
#     StructField("nationality", StringType()),
#     StructField("location", StringType()),
#     StructField("graduation_year", IntegerType()),
#     StructField("gpa", FloatType()),
#     StructField("education_level", StringType()),
#     StructField("job_category", StringType()),
#     StructField("current_title", StringType()),
#     StructField("seniority", StringType()),
#     StructField("experience_range", StringType()),
#     StructField("current_company", StringType()),
#     StructField("previous_companies", ArrayType(StringType())),
#     StructField("previous_industries", ArrayType(StringType())),
#     StructField("skills", ArrayType(StringType())),
#     StructField("certifications", ArrayType(StringType())),
#     StructField("current_salary", IntegerType()),
#     StructField("is_active", BooleanType()),
#     StructField("created_at", LongType()),
#     StructField("updated_at", LongType()),
#     StructField("version", IntegerType()),
# ])

# JOB_AFTER_SCHEMA = StructType([
#     StructField("post_id", StringType()),
#     StructField("company", StringType()),
#     StructField("category", StringType()),
#     StructField("job_title", StringType()),
#     StructField("application_method", StringType()),
#     StructField("post_date", LongType()),
#     StructField("deadline", LongType()),
#     StructField("certifications", ArrayType(StringType())),
#     StructField("skills_needed", ArrayType(StringType())),
#     StructField("platform", StringType()),
#     StructField("employment_type", StringType()),
#     StructField("city", StringType()),
#     StructField("country", StringType()),
#     StructField("language", StringType()),
#     StructField("work_mode", StringType()),
#     StructField("industry", StringType()),
#     StructField("detailed_experience_level", StringType()),
#     StructField("salary", IntegerType()),
#     StructField("years_experience", StringType()),
#     StructField("status", StringType()),
#     StructField("lifecycle_stage", IntegerType()),
#     StructField("created_at", LongType()),
#     StructField("updated_at", LongType()),
#     StructField("version", IntegerType()),
# ])

# APPLICATION_AFTER_SCHEMA = StructType([
#     StructField("application_id", StringType()),
#     StructField("candidate_id", StringType()),
#     StructField("post_id", StringType()),
#     StructField("status", StringType()),
#     StructField("lifecycle_stage", IntegerType()),
#     StructField("applied_date", LongType()),
#     StructField("last_updated", LongType()),
#     StructField("final_outcome", StringType()),
#     StructField("created_at", LongType()),
#     StructField("updated_at", LongType()),
#     StructField("version", IntegerType()),
# ])

# # Schema of the `unresolved` DataFrame written to pending_applications in
# # process_application_batch (step 10). Kept as an explicit top-level
# # constant so it can be used to pre-create the Delta table via
# # ensure_delta_table() (pure JVM, addColumns() only - no data written)
# # instead of ever falling back to spark.createDataFrame([], schema) for a
# # schema-only empty DataFrame. That fallback is what was crashing: even an
# # empty Python list goes through sc.parallelize() -> PythonRDD, which still
# # schedules one real task per default-parallelism partition, and each task
# # still spawns a Python worker to do a (zero-row) handshake - which is
# # exactly the worker that was dying on Windows. Pre-creating the table
# # means that branch is never taken.
# PENDING_APPLICATIONS_SCHEMA = StructType([
#     StructField("application_id", StringType()),
#     StructField("candidate_id", StringType()),
#     StructField("post_id", StringType()),
#     StructField("applied_date", TimestampType()),
#     StructField("event_ts", TimestampType()),
#     StructField("retry_count", IntegerType()),
# ])


# def envelope_schema(after_schema: StructType) -> StructType:
#     return StructType([
#         StructField("before", after_schema),
#         StructField("after", after_schema),
#         StructField("op", StringType()),
#         StructField("ts_ms", LongType()),
#         StructField("source", StructType([
#             StructField("table", StringType()),
#             StructField("lsn", LongType()),
#         ])),
#     ])


# def read_cdc_stream(spark: SparkSession, topic: str, after_schema: StructType,
#                      ts_cols: list, starting_offsets: str = "earliest") -> DataFrame:
#     raw = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#         .option("subscribe", topic)
#         .option("startingOffsets", starting_offsets)
#         .option("failOnDataLoss", "false")
#         .load()
#     )

#     parsed = raw.select(
#         F.from_json(F.col("value").cast("string"), envelope_schema(after_schema)).alias("env")
#     )

#     df = parsed.select(
#         F.col("env.op").alias("op"),
#         (F.col("env.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
#         F.col("env.after.*"),
#     )

#     for c in ts_cols:
#         df = df.withColumn(c, (F.col(c) / 1_000_000).cast("timestamp"))

#     return df.filter(F.col("op").isNotNull())

# def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, extra_cols: dict):
#     if DeltaTable.isDeltaTable(spark, path):
#         return
#     full_schema = StructType(schema.fields + [
#         StructField(name, dtype, True) for name, dtype in extra_cols.items()
#     ])
#     # Create _delta_log only — no task, no Parquet file, no native DLL extraction
#     DeltaTable.createIfNotExists(spark) \
#         .addColumns(full_schema) \
#         .location(path) \
#         .execute()
# # def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, extra_cols: dict):
# #     if DeltaTable.isDeltaTable(spark, path):
# #         return
# #     full_schema = StructType(schema.fields + [
# #         StructField(name, dtype, True) for name, dtype in extra_cols.items()
# #     ])
# #     empty = spark.createDataFrame([], full_schema)
# #     empty.write.format("delta").mode("overwrite").save(path)


# def _df_is_empty(df: DataFrame) -> bool:
#     return df.isEmpty()


# def upsert_dim_batch(batch_df: DataFrame, batch_id: int, delta_path: str, key_col: str):
#     if _df_is_empty(batch_df):
#         print(f"[batch {batch_id}] No rows for {delta_path}")
#         return

#     spark = batch_df.sparkSession
#     # Cache filtered subsets so they are computed once and reused across merge + count
#     #
#     # DEDUPE BEFORE MERGE:
#     # A CDC micro-batch can contain multiple events for the same key
#     # (created, then updated one or more times) - especially the very
#     # first "earliest" batch, which can replay a key's entire history at
#     # once. Delta's MERGE rejects multiple source rows matching the same
#     # target row ([DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]),
#     # even with a `s.version >= t.version` condition on whenMatchedUpdateAll -
#     # that condition can be true for several of those duplicate rows at
#     # once, which is exactly the ambiguity Delta is refusing to resolve for
#     # you. So collapse to one row per key (the highest `version`) before
#     # ever building the merge.
#     upserts_raw = batch_df.filter(F.col("op").isin("c", "r", "u")).drop("op", "event_ts")
#     upserts = (
#         upserts_raw
#         .withColumn(
#             "_rn",
#             F.row_number().over(
#                 Window.partitionBy(key_col).orderBy(F.col("version").desc())
#             )
#         )
#         .filter(F.col("_rn") == 1)
#         .drop("_rn")
#         .cache()
#     )
#     # Deletes only carry the key column, so any duplicate delete events for
#     # the same key are identical rows - dropDuplicates is enough here (no
#     # "latest version wins" ordering needed since there's nothing to order by).
#     deletes = batch_df.filter(F.col("op") == "d").select(key_col).dropDuplicates([key_col]).cache()

#     target = DeltaTable.forPath(spark, delta_path)

#     if not _df_is_empty(upserts):
#         (target.alias("t")
#          .merge(upserts.alias("s"), f"t.{key_col} = s.{key_col}")
#          .whenMatchedUpdateAll(condition="s.version >= t.version")
#          .whenNotMatchedInsertAll()
#          .execute())
#         print(f"[batch {batch_id}] Upserted {upserts.count()} rows into {delta_path}")

#     if not _df_is_empty(deletes):
#         (target.alias("t")
#          .merge(deletes.alias("s"), f"t.{key_col} = s.{key_col}")
#          .whenMatchedUpdate(set={"is_active": F.lit(False)})
#          .execute())
#         print(f"[batch {batch_id}] Soft-deleted {deletes.count()} rows in {delta_path}")

#     upserts.unpersist()
#     deletes.unpersist()


# def start_dimension_writers(spark: SparkSession):
#     candidate_path = f"{DELTA_ROOT}/candidate_dim"
#     job_path = f"{DELTA_ROOT}/job_dim"

#     ensure_delta_table(spark, candidate_path, CANDIDATE_AFTER_SCHEMA, {})
#     ensure_delta_table(spark, job_path, JOB_AFTER_SCHEMA, {})

#     # Same note as start_matching_query: 'earliest' only governs the
#     # initial (no-checkpoint-yet) run; every later run resumes from
#     # CHECKPOINT_ROOT/{candidate_dim,job_dim} automatically.
#     candidate_stream = read_cdc_stream(
#         spark, TOPICS["candidate"], CANDIDATE_AFTER_SCHEMA,
#         ["created_at", "updated_at"], starting_offsets="earliest"
#     )
#     job_stream = read_cdc_stream(
#         spark, TOPICS["job"], JOB_AFTER_SCHEMA,
#         ["post_date", "deadline", "created_at", "updated_at"], starting_offsets="earliest"
#     )

#     q1 = (
#         candidate_stream.writeStream
#         .queryName("candidate_dim_writer")
#         .foreachBatch(lambda df, bid: upsert_dim_batch(df, bid, candidate_path, "candidate_id"))
#         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/candidate_dim")
#         .trigger(processingTime="30 seconds")
#         .start()
#     )
#     q2 = (
#         job_stream.writeStream
#         .queryName("job_dim_writer")
#         .foreachBatch(lambda df, bid: upsert_dim_batch(df, bid, job_path, "post_id"))
#         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job_dim")
#         .trigger(processingTime="30 seconds")
#         .start()
#     )
#     return q1, q2, candidate_path, job_path



# # ---------------------------------------------------------------------------
# # Native Spark scoring expressions (replaces the previous Python UDFs).
# #
# # Every UDF used to force a round-trip: JVM -> serialize -> Python worker ->
# # deserialize -> back to JVM, for every single row. That serialization
# # overhead (plus the Python worker process itself) was almost certainly the
# # biggest cost in this job, and it's also the most likely explanation for
# # the Python worker crashes you were seeing under load: UDF rows are
# # processed by a separate Python process that Spark has much less control
# # over (memory accounting, retries, etc.) than it does over native JVM code.
# #
# # Below, every score is expressed with built-in Spark SQL functions
# # (split/lower/trim/array_intersect/size/regexp_extract_all/when/otherwise/
# # element_at/transform/array_min/array_max) so everything stays inside the
# # JVM and Catalyst can optimize/whole-stage-codegen it.
# # ---------------------------------------------------------------------------

# _NUM_RE = r"(\d+(?:\.\d+)?)"


# def _norm_array(col_name: str):
#     """Lowercase + trim every element of a string array column, treating
#     null/empty arrays as empty (mirrors the old `have`/`need` set logic)."""
#     return F.transform(F.coalesce(F.col(col_name), F.array()), lambda x: F.trim(F.lower(x)))


# def skill_score_expr(candidate_skills_col: str, required_skills_col: str):
#     need = _norm_array(required_skills_col)
#     have = _norm_array(candidate_skills_col)
#     overlap = F.size(F.array_intersect(have, need))
#     return (
#         F.when(F.size(need) == 0, F.lit(100.0))
#         .when(F.size(have) == 0, F.lit(0.0))
#         .otherwise(F.round(F.lit(100.0) * overlap / F.size(need), 1))
#     )


# def experience_score_expr(candidate_text_col: str, job_text_col: str):
#     def bounds(text_col_name):
#         nums = F.transform(
#             F.regexp_extract_all(F.coalesce(F.col(text_col_name), F.lit("")), F.lit(_NUM_RE), 1),
#             lambda x: x.cast("double"),
#         )
#         n = F.size(nums)
#         has_plus = F.instr(F.coalesce(F.col(text_col_name), F.lit("")), "+") > 0
#         lo = F.array_min(nums)
#         hi = F.when((n == 1) & has_plus, F.array_min(nums) + F.lit(8.0)).otherwise(F.array_max(nums))
#         valid = n > 0
#         return valid, lo, hi

#     c_valid, c_lo, c_hi = bounds(candidate_text_col)
#     j_valid, j_lo, j_hi = bounds(job_text_col)

#     c_mid = (c_lo + c_hi) / F.lit(2.0)
#     gap = j_lo - c_mid
#     over = c_mid - j_hi

#     in_range_score = F.lit(100.0)
#     below_score = F.greatest(F.lit(0.0), F.lit(100.0) - F.lit(20.0) * gap)
#     above_score = F.greatest(F.lit(50.0), F.lit(100.0) - F.lit(8.0) * over)

#     return (
#         F.when(~(c_valid & j_valid), F.lit(50.0))
#         .when((c_mid >= j_lo) & (c_mid <= j_hi), F.round(in_range_score, 1))
#         .when(c_mid < j_lo, F.round(below_score, 1))
#         .otherwise(F.round(above_score, 1))
#     )


# def salary_score_expr(candidate_salary_col: str, job_salary_col: str):
#     cand = F.col(candidate_salary_col)
#     job = F.col(job_salary_col)
#     ratio = job / cand
#     return (
#         F.when(cand.isNull() | job.isNull() | (cand <= 0), F.lit(50.0))
#         .when(ratio >= 1.15, F.lit(100.0))
#         .when(ratio >= 1.0, F.lit(85.0))
#         .when(ratio >= 0.90, F.lit(55.0))
#         .otherwise(F.lit(20.0))
#     )


# def location_score_expr(candidate_location_col: str, job_city_col: str, job_country_col: str, work_mode_col: str):
#     parts = F.split(F.trim(F.lower(F.coalesce(F.col(candidate_location_col), F.lit("")))), ",")
#     cand_city = F.trim(F.element_at(parts, 1))
#     cand_country = F.trim(F.element_at(parts, -1))
#     jc = F.trim(F.lower(F.coalesce(F.col(job_city_col), F.lit(""))))
#     jco = F.trim(F.lower(F.coalesce(F.col(job_country_col), F.lit(""))))
#     is_remote = F.lower(F.trim(F.coalesce(F.col(work_mode_col), F.lit("")))) == "remote"
#     has_location_data = F.col(candidate_location_col).isNotNull() & (
#         F.col(job_city_col).isNotNull() | F.col(job_country_col).isNotNull()
#     )

#     return (
#         F.when(is_remote, F.lit(100.0))
#         .when(~has_location_data, F.lit(50.0))
#         .when((cand_city != "") & (jc != "") & (cand_city == jc), F.lit(100.0))
#         .when((cand_country != "") & (jco != "") & (cand_country == jco), F.lit(55.0))
#         .otherwise(F.lit(15.0))
#     )


# def _country_lang_map():
#     # Built lazily (not at module import time): F.lit()/F.create_map() need
#     # an active SparkContext/JVM gateway, which only exists after
#     # build_spark() has run. Building this at import time is what caused
#     # `AssertionError` from pyspark's `_invoke_function` before any
#     # SparkSession existed.
#     return F.create_map(
#         [F.lit(x) for pair in COUNTRY_LANGUAGES.items() for x in pair]
#     )


# def language_score_expr(nationality_col: str, job_language_col: str):
#     job_lang = F.trim(F.lower(F.col(job_language_col)))
#     inferred = F.element_at(
#         _country_lang_map(),
#         F.trim(F.lower(F.coalesce(F.col(nationality_col), F.lit("")))),
#     )

#     return (
#         F.when(F.col(job_language_col).isNull(), F.lit(100.0))
#         .when(inferred.isNull(), F.lit(50.0))
#         .when(inferred == job_lang, F.lit(100.0))
#         .when(job_lang == "english", F.lit(60.0))
#         .otherwise(F.lit(25.0))
#     )


# def priority_expr(score_col: str):
#     return (
#         F.when(F.col(score_col).isNull(), F.lit("UNSCORED"))
#         .when(F.col(score_col) >= PRIORITY_THRESHOLDS["HIGH"], F.lit("HIGH_PRIORITY"))
#         .when(F.col(score_col) >= PRIORITY_THRESHOLDS["MEDIUM"], F.lit("MEDIUM_PRIORITY"))
#         .otherwise(F.lit("LOW_PRIORITY"))
#     )


# def score_applications(joined: DataFrame) -> DataFrame:
#     scored = (
#         joined
#         .withColumn("skill_score", skill_score_expr("skills", "skills_needed"))
#         .withColumn("experience_score", experience_score_expr("experience_range", "years_experience"))
#         .withColumn("salary_score", salary_score_expr("current_salary", "salary"))
#         .withColumn("location_score", location_score_expr("location", "city", "country", "work_mode"))
#         .withColumn("language_score", language_score_expr("nationality", "language"))
#         .withColumn(
#             "overall_score",
#             F.round(
#                 F.col("skill_score") * MATCH_WEIGHTS["skills"]
#                 + F.col("experience_score") * MATCH_WEIGHTS["experience"]
#                 + F.col("salary_score") * MATCH_WEIGHTS["salary"]
#                 + F.col("location_score") * MATCH_WEIGHTS["location"]
#                 + F.col("language_score") * MATCH_WEIGHTS["language"],
#                 1,
#             ),
#         )
#         .withColumn("priority", priority_expr("overall_score"))
#         .withColumn("scored_at", F.current_timestamp())
#     )
#     return scored


# import time
# import datetime


# def _step_log(batch_id: int, step: str, msg: str = ""):
#     """Flush a timestamped line immediately so it lands in the console/log
#     file even if the process crashes right after this step. Without
#     flush=True, buffered stdout can be lost entirely when the JVM/worker
#     dies, which is exactly what makes 'which step crashed' hard to tell
#     from the raw stack trace alone."""
#     ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
#     line = f"[{ts}] [batch {batch_id}] STEP {step}"
#     if msg:
#         line += f" -- {msg}"
#     print(line, flush=True)
#     sys.stdout.flush()


# def _alias_dim_cols(df: DataFrame, prefix: str, exclude: list) -> DataFrame:
#     """Rename all columns with a prefix, except excluded keys."""
#     for col_name in df.columns:
#         if col_name not in exclude:
#             df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
#     return df

# # ---------------------------------------------------------------------------
# # Batch metrics
# #
# # Design goal: capture the metrics below WITHOUT adding new Spark actions
# # beyond what the pipeline already needs. Every field here is filled in one
# # of these ways:
# #   - read off a DataFrame that's already cached/materialized by an action
# #     that was going to run anyway (.count() on a cached df is a cache hit,
# #     not a re-scan)
# #   - read from Delta's own transaction history (operationMetrics), which
# #     Delta computes and commits as part of every write/merge for free
# #   - read from Catalyst's plan statistics (file/table size metadata),
# #     which does not scan any data
# #   - read from the local Spark UI's REST API (a plain HTTP call, not a
# #     Spark action) for shuffle bytes
# # The only genuinely new per-batch actions are the .count() calls on
# # already-cached DataFrames, which are effectively free.
# # ---------------------------------------------------------------------------

# import json
# import urllib.request
# from dataclasses import dataclass, asdict
# from typing import Optional

# METRICS_LOG_PATH = os.getenv("METRICS_LOG_PATH", f"{DELTA_ROOT}/_metrics/batch_metrics.jsonl")


# @dataclass
# class BatchMetrics:
#     batch_id: int
#     batch_start_time: str = ""
#     batch_end_time: str = ""
#     batch_duration_sec: float = 0.0
#     status: str = "running"

#     incoming_rows: int = 0
#     resolved_rows: int = 0
#     unresolved_rows: int = 0
#     scored_rows: int = 0
#     high_priority_rows: int = 0

#     candidate_dim_total_rows: int = 0
#     candidate_dim_updated_rows: int = 0

#     job_dim_total_rows: int = 0
#     job_dim_updated_rows: int = 0

#     candidate_dim_broadcast_mb: float = 0.0
#     job_dim_broadcast_mb: float = 0.0

#     pending_write_sec: float = 0.0
#     resolved_check_sec: float = 0.0
#     scoring_write_sec: float = 0.0
#     high_priority_check_sec: float = 0.0

#     files_read: int = 0
#     files_written: int = 0

#     shuffle_read_mb: float = 0.0
#     shuffle_write_mb: float = 0.0

#     input_rows_per_sec: float = 0.0

#     def finalize(self, start_ts: float, end_ts: float):
#         self.batch_duration_sec = round(end_ts - start_ts, 3)
#         if self.batch_duration_sec > 0:
#             self.input_rows_per_sec = round(self.incoming_rows / self.batch_duration_sec, 2)

#     def log_line(self) -> str:
#         return json.dumps(asdict(self), default=str)


# class _Timer:
#     """Context manager: records elapsed wall-clock seconds into
#     metrics.<field_name>. Wrap exactly the code that performs the Spark
#     action you want timed - nothing else."""

#     def __init__(self, metrics: "BatchMetrics", field_name: str):
#         self.metrics = metrics
#         self.field_name = field_name
#         self._start = None

#     def __enter__(self):
#         self._start = time.time()
#         return self

#     def __exit__(self, exc_type, exc, tb):
#         setattr(self.metrics, self.field_name, round(time.time() - self._start, 3))
#         return False


# def _plan_size_mb(df: DataFrame) -> float:
#     """Catalyst's estimated size of a DataFrame's plan, in MB. Reads size
#     stats already tracked in table/file metadata - does not scan data.
#     Best-effort: falls back to 0.0 if the internal API shape changes."""
#     try:
#         size_bytes = int(str(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))
#         return round(size_bytes / (1024 * 1024), 3)
#     except Exception:
#         return 0.0


# def _count_input_files(df: DataFrame) -> int:
#     """Number of files backing this DataFrame's plan. Triggers file
#     listing/metadata resolution only, never a data scan."""
#     try:
#         return len(df.inputFiles())
#     except Exception:
#         return 0


# def _delta_last_operation_metrics(spark: SparkSession, path: str) -> dict:
#     """operationMetrics from the most recent commit to a Delta table -
#     Delta already computed these (numOutputRows, numOutputFiles,
#     numTargetRowsUpdated, numTargetRowsInserted, ...) while performing the
#     write/merge; this just reads the transaction log, no extra Spark job."""
#     try:
#         if not DeltaTable.isDeltaTable(spark, path):
#             return {}
#         rows = (
#             DeltaTable.forPath(spark, path)
#             .history(1)
#             .select("operationMetrics")
#             .collect()
#         )
#         if rows:
#             return dict(rows[0]["operationMetrics"] or {})
#     except Exception:
#         pass
#     return {}


# def _m_int(metrics_dict: dict, *keys) -> Optional[int]:
#     for k in keys:
#         v = metrics_dict.get(k)
#         if v is not None:
#             try:
#                 return int(v)
#             except (TypeError, ValueError):
#                 pass
#     return None


# def _shuffle_metrics_since(spark: SparkSession, since_ms: float) -> tuple:
#     """Best-effort shuffle read/write MB for stages completed after
#     `since_ms`, pulled from the local Spark UI's REST API - a loopback HTTP
#     call, not a Spark action. Returns (0.0, 0.0) if the UI is disabled
#     (spark.ui.enabled=false) or unreachable, so this never breaks a batch."""
#     try:
#         ui_url = spark.sparkContext.uiWebUrl
#         if not ui_url:
#             return 0.0, 0.0
#         app_id = spark.sparkContext.applicationId
#         req_url = f"{ui_url}/api/v1/applications/{app_id}/stages?status=complete"
#         with urllib.request.urlopen(req_url, timeout=2) as resp:
#             stages = json.loads(resp.read())
#         read_bytes = 0
#         write_bytes = 0
#         for st in stages:
#             completion = st.get("completionTime")
#             if not completion:
#                 continue
#             completion_ms = datetime.datetime.strptime(
#                 completion.split(".")[0], "%Y-%m-%dT%H:%M:%S"
#             ).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
#             if completion_ms >= since_ms:
#                 read_bytes += st.get("shuffleReadBytes", 0) or 0
#                 write_bytes += st.get("shuffleWriteBytes", 0) or 0
#         return round(read_bytes / (1024 * 1024), 3), round(write_bytes / (1024 * 1024), 3)
#     except Exception:
#         return 0.0, 0.0


# def _emit_batch_metrics(metrics: "BatchMetrics"):
#     """Log the metrics record and append it as a JSON line to disk.
#     Deliberately plain Python file I/O, NOT a Spark write: this file's own
#     comments explain that even a single-row spark.createDataFrame(...) spins
#     up a Python worker task, which is exactly the class of Windows/AV crash
#     this pipeline was hardened against elsewhere. Metrics logging should
#     never be able to take the streaming query down."""
#     line = metrics.log_line()
#     print(f"[batch {metrics.batch_id}] METRICS {line}", flush=True)
#     try:
#         os.makedirs(os.path.dirname(METRICS_LOG_PATH), exist_ok=True)
#         with open(METRICS_LOG_PATH, "a", encoding="utf-8") as f:
#             f.write(line + "\n")
#     except Exception as e:
#         print(f"[batch {metrics.batch_id}] WARNING: failed to write metrics log: {e}", flush=True)


# def process_application_batch(
#     app_batch: DataFrame,
#     batch_id: int,
#     candidate_path: str,
#     job_path: str,
#     results_path: str,
#     pending_path: str
# ):
#     spark = app_batch.sparkSession
#     batch_start_ts = time.time()
#     batch_start_wall = datetime.datetime.now()
#     metrics = BatchMetrics(batch_id=batch_id, batch_start_time=batch_start_wall.isoformat())

#     # Everything we .cache()/.persist() goes here so it always gets
#     # unpersisted, even on an early return - the old version leaked cached
#     # dimension tables on every "no resolved applications" batch.
#     persisted = []

#     def _finish(status: str):
#         metrics.status = status
#         metrics.batch_end_time = datetime.datetime.now().isoformat()
#         metrics.finalize(batch_start_ts, time.time())
#         metrics.shuffle_read_mb, metrics.shuffle_write_mb = _shuffle_metrics_since(
#             spark, batch_start_wall.timestamp() * 1000
#         )
#         for df in persisted:
#             try:
#                 df.unpersist()
#             except Exception:
#                 pass
#         _emit_batch_metrics(metrics)

#     _step_log(batch_id, "0-start", "process_application_batch entered")

#     # ============================================================
#     # 1. Get new applications
#     # ============================================================

#     new_apps = (
#         app_batch
#         .filter(F.col("op").isin("c", "r"))
#         .select(
#             "application_id",
#             "candidate_id",
#             "post_id",
#             "applied_date",
#             "event_ts"
#         )
#     )
#     _step_log(batch_id, "1-new_apps-built", "lazy plan built (no action yet)")

#     if _df_is_empty(new_apps):
#         _step_log(batch_id, "1-new_apps-empty", "returning early")
#         print(f"[batch {batch_id}] No new applications")
#         _finish("no_new_apps")
#         return
#     _step_log(batch_id, "1-new_apps-checked", "not empty, continuing")

#     # ============================================================
#     # 2. Read pending applications
#     # ============================================================

#     pending_table_exists = DeltaTable.isDeltaTable(spark, pending_path)
#     if pending_table_exists:
#         pending = (
#             spark.read
#             .format("delta")
#             .load(pending_path)
#         )
#     else:
#         pending_schema = (
#             new_apps
#             .withColumn("retry_count", F.lit(0))
#             .schema
#         )
#         pending = spark.createDataFrame(
#             spark.sparkContext.emptyRDD(),
#             pending_schema
#         )
#     metrics.files_read += _count_input_files(pending)
#     _step_log(batch_id, "2-pending-read", f"pending source ready (existing_delta_table={pending_table_exists})")

#     # ============================================================
#     # 3. Combine new applications + pending applications
#     #
#     # OPTIMIZATION: cache `incoming` immediately. It's read twice below
#     # (the isEmpty() check, then the join that builds `enriched`) - without
#     # caching, both reads recompute the new_apps/pending union from scratch.
#     # ============================================================

#     incoming = (
#         new_apps
#         .withColumn("retry_count", F.lit(0))
#         .unionByName(
#             pending,
#             allowMissingColumns=True
#         )
#         .cache()
#     )
#     persisted.append(incoming)
#     _step_log(batch_id, "3-incoming-built", "lazy plan built, cached (reused by isEmpty() + join below)")

#     if _df_is_empty(incoming):
#         _step_log(batch_id, "3-incoming-empty", "returning early")
#         print(f"[batch {batch_id}] No incoming applications")
#         _finish("no_incoming")
#         return
#     _step_log(batch_id, "3-incoming-checked", "not empty, continuing")

#     # `incoming` is already cached and materialized by isEmpty() above, so
#     # this count is a cache hit, not a fresh scan of new_apps/pending.
#     metrics.incoming_rows = incoming.count()

#     # ============================================================
#     # 4. Read candidate dimension
#     # ============================================================

#     CANDIDATE_JOIN_COLS = [
#         "candidate_id",
#         "name",
#         "skills",
#         "experience_range",
#         "current_salary",
#         "location",
#         "nationality",
#     ]

#     candidate_dim_raw = (
#         spark.read
#         .format("delta")
#         .load(candidate_path)
#         .filter(F.col("is_active") == True)
#         .select(*CANDIDATE_JOIN_COLS)
#         .withColumn(
#             "_dim_present",
#             F.lit(True)
#         )
#     )
#     metrics.files_read += _count_input_files(candidate_dim_raw)
#     _step_log(batch_id, "4-candidate_dim-read", "lazy plan built")

#     # ============================================================
#     # 5. Read job dimension
#     # ============================================================

#     JOB_JOIN_COLS = [
#         "post_id",
#         "company",
#         "job_title",
#         "skills_needed",
#         "years_experience",
#         "salary",
#         "city",
#         "country",
#         "language",
#         "work_mode",
#     ]

#     job_dim_raw = (
#         spark.read
#         .format("delta")
#         .load(job_path)
#         .filter(F.col("status") != "closed")
#         .select(*JOB_JOIN_COLS)
#         .withColumn(
#             "_dim_present",
#             F.lit(True)
#         )
#     )
#     metrics.files_read += _count_input_files(job_dim_raw)
#     _step_log(batch_id, "5-job_dim-read", "lazy plan built")

#     # ============================================================
#     # 6. Alias + cache dimension columns
#     #
#     # OPTIMIZATION: cache both broadcast dimensions. They're materialized
#     # once for the broadcast join; caching means the row-count metrics
#     # below are cache hits instead of a second Delta scan.
#     # ============================================================

#     candidate_dim_aliased = _alias_dim_cols(
#         candidate_dim_raw,
#         "cand",
#         ["candidate_id"]
#     )
#     job_dim_aliased = _alias_dim_cols(
#         job_dim_raw,
#         "job",
#         ["post_id"]
#     )
#     # Catalyst's plan-stats size estimate, read from table metadata before
#     # caching changes the plan shape - free, no data scan.
#     metrics.candidate_dim_broadcast_mb = _plan_size_mb(candidate_dim_aliased)
#     metrics.job_dim_broadcast_mb = _plan_size_mb(job_dim_aliased)

#     candidate_dim = candidate_dim_aliased.cache()
#     job_dim = job_dim_aliased.cache()
#     persisted.extend([candidate_dim, job_dim])
#     _step_log(batch_id, "6-aliased", "lazy plan built, dims cached (broadcast join reuses this)")

#     # ============================================================
#     # 7. Join applications with dimensions
#     # ============================================================

#     enriched = (
#         incoming
#         .join(
#             F.broadcast(candidate_dim),
#             "candidate_id",
#             "left"
#         )
#         .join(
#             F.broadcast(job_dim),
#             "post_id",
#             "left"
#         )
#     )
#     _step_log(batch_id, "7-enriched-built", "lazy plan built (broadcast joins)")

#     # ============================================================
#     # 8. Identify resolved applications
#     #
#     # OPTIMIZATION (the biggest one from the log analysis): cache `resolved`
#     # immediately. It was previously read twice - once for isEmpty(), once
#     # again (via scored_input) to build `scored` - which recomputed the
#     # entire join+filter lineage a second time. Caching here means the
#     # isEmpty() check below pays the cost once, and scoring reuses it free.
#     # ============================================================

#     resolved = enriched.filter(
#         F.col("cand__dim_present").isNotNull()
#         &
#         F.col("job__dim_present").isNotNull()
#     ).cache()
#     persisted.append(resolved)
#     _step_log(batch_id, "8-resolved-built", "lazy plan built, cached to avoid recomputation at isEmpty()+scoring")

#     # ============================================================
#     # 9. Identify unresolved applications
#     # ============================================================

#     unresolved = (
#         enriched
#         .filter(
#             F.col("cand__dim_present").isNull()
#             |
#             F.col("job__dim_present").isNull()
#         )
#         .select(
#             "application_id",
#             "candidate_id",
#             "post_id",
#             "applied_date",
#             "event_ts",
#             "retry_count"
#         )
#         .withColumn(
#             "retry_count",
#             F.col("retry_count") + 1
#         )
#         .filter(
#             F.col("retry_count") <= MAX_ENRICHMENT_RETRIES
#         )
#     )
#     _step_log(batch_id, "9-unresolved-built", "lazy plan built (no action yet)")

#     # ============================================================
#     # 10. Persist unresolved applications
#     #
#     # IMPORTANT:
#     # Do NOT cache unresolved.
#     # Do NOT count unresolved.
#     # The write itself is the Spark action.
#     # Row/file counts for metrics come from Delta's own transaction history
#     # below (operationMetrics), not from an extra .count() action.
#     # ============================================================

#     _step_log(batch_id, "10-pending-write-BEGIN", f"about to trigger the .save() action, table_exists={pending_table_exists}")

#     with _Timer(metrics, "pending_write_sec"):
#         (
#             unresolved
#             .write
#             .format("delta")
#             .mode("overwrite")
#             .save(pending_path)
#         )

#     if pending_table_exists:
#         _step_log(batch_id, "10-pending-write-END", "save() returned successfully (update path)")
#         print(
#             f"[batch {batch_id}] "
#             f"Pending applications updated"
#         )
#     else:
#         _step_log(batch_id, "10-pending-write-END", "save() returned successfully (create path)")
#         print(
#             f"[batch {batch_id}] "
#             f"Created pending applications table"
#         )

#     pending_write_metrics = _delta_last_operation_metrics(spark, pending_path)
#     metrics.unresolved_rows = _m_int(pending_write_metrics, "numOutputRows") or 0
#     metrics.files_written += _m_int(pending_write_metrics, "numOutputFiles", "numFiles") or 0

#     # ============================================================
#     # 11. Check resolved applications
#     #
#     # Keep this lightweight.
#     # _df_is_empty() should use limit(1), not count().
#     # `resolved` is cached above, so this no longer re-executes the
#     # join/filter lineage.
#     # ============================================================

#     _step_log(batch_id, "11-resolved-isEmpty-BEGIN", "about to call isEmpty() on resolved (cached)")
#     with _Timer(metrics, "resolved_check_sec"):
#         resolved_is_empty = _df_is_empty(resolved)
#     _step_log(batch_id, "11-resolved-isEmpty-END", f"resolved_is_empty={resolved_is_empty}")

#     if resolved_is_empty:

#         print(
#             f"[batch {batch_id}] "
#             f"No resolved applications"
#         )
#         _finish("no_resolved")
#         return

#     # `resolved`/`candidate_dim`/`job_dim` are all cached and already
#     # materialized (by isEmpty()/the join above), so these counts are cache
#     # hits, not fresh scans.
#     metrics.resolved_rows = resolved.count()
#     metrics.candidate_dim_total_rows = candidate_dim.count()
#     metrics.job_dim_total_rows = job_dim.count()

#     # candidate_dim/job_dim are upserted by separate concurrent streaming
#     # queries (candidate_dim_writer/job_dim_writer), not by this batch. This
#     # reads whatever the most recent MERGE into each table committed - a
#     # useful proxy for "how much dimension churn is happening", but it isn't
#     # guaranteed to be exactly this batch's own upsert.
#     last_cand_merge = _delta_last_operation_metrics(spark, candidate_path)
#     last_job_merge = _delta_last_operation_metrics(spark, job_path)
#     metrics.candidate_dim_updated_rows = (
#         (_m_int(last_cand_merge, "numTargetRowsUpdated") or 0)
#         + (_m_int(last_cand_merge, "numTargetRowsInserted") or 0)
#     )
#     metrics.job_dim_updated_rows = (
#         (_m_int(last_job_merge, "numTargetRowsUpdated") or 0)
#         + (_m_int(last_job_merge, "numTargetRowsInserted") or 0)
#     )

#     # ============================================================
#     # 12. Rename columns for scoring (reuses cached `resolved`)
#     # ============================================================

#     scored_input = (
#         resolved
#         .withColumnRenamed(
#             "cand_skills",
#             "skills"
#         )
#         .withColumnRenamed(
#             "job_skills_needed",
#             "skills_needed"
#         )
#         .withColumnRenamed(
#             "cand_experience_range",
#             "experience_range"
#         )
#         .withColumnRenamed(
#             "job_years_experience",
#             "years_experience"
#         )
#         .withColumnRenamed(
#             "cand_current_salary",
#             "current_salary"
#         )
#         .withColumnRenamed(
#             "job_salary",
#             "salary"
#         )
#         .withColumnRenamed(
#             "cand_location",
#             "location"
#         )
#         .withColumnRenamed(
#             "job_city",
#             "city"
#         )
#         .withColumnRenamed(
#             "job_country",
#             "country"
#         )
#         .withColumnRenamed(
#             "job_work_mode",
#             "work_mode"
#         )
#         .withColumnRenamed(
#             "cand_nationality",
#             "nationality"
#         )
#         .withColumnRenamed(
#             "job_language",
#             "language"
#         )
#     )
#     _step_log(batch_id, "12-scored_input-built", "lazy plan built (reuses cached `resolved`)")

#     # ============================================================
#     # 13. Score applications
#     #
#     # OPTIMIZATION: cache `scored` immediately. It was previously read
#     # twice - once for the results write, once again (filtered) for
#     # high_priority.isEmpty() - which recomputed the whole scoring
#     # transformation a second time. This is the same class of bug as #8,
#     # and is the most likely explanation for the ~3 sec high_priority
#     # isEmpty() cost even when the result was empty.
#     # ============================================================

#     scored = score_applications(scored_input).cache()
#     persisted.append(scored)
#     _step_log(batch_id, "13-scored-built", "lazy plan built, cached to avoid recomputation at high_priority isEmpty()")

#     # ============================================================
#     # 14. Write results
#     # ============================================================

#     _step_log(batch_id, "14-results-write-BEGIN", "about to trigger the .save() action")

#     with _Timer(metrics, "scoring_write_sec"):
#         (
#             scored
#             .write
#             .format("delta")
#             .mode("append")
#             .option(
#                 "mergeSchema",
#                 "true"
#             )
#             .save(results_path)
#         )
#     _step_log(batch_id, "14-results-write-END", "save() returned successfully")

#     print(
#         f"[batch {batch_id}] "
#         f"Scored applications written to results"
#     )

#     results_write_metrics = _delta_last_operation_metrics(spark, results_path)
#     # Prefer Delta's own commit metrics; fall back to counting the cached
#     # `scored` DataFrame (cheap - it's already materialized) only if Delta
#     # didn't report a row count.
#     metrics.scored_rows = _m_int(results_write_metrics, "numOutputRows")
#     if metrics.scored_rows is None:
#         metrics.scored_rows = scored.count()
#     metrics.files_written += _m_int(results_write_metrics, "numOutputFiles", "numFiles") or 0

#     # ============================================================
#     # 15. Find high-priority matches (filters the cached `scored`)
#     # ============================================================

#     high_priority = (
#         scored
#         .filter(
#             F.col("priority") == "HIGH_PRIORITY"
#         )
#         .select(
#             "application_id",
#             "candidate_id",
#             F.col("cand_name").alias(
#                 "candidate_name"
#             ),
#             "post_id",
#             F.col("job_job_title").alias(
#                 "job_title"
#             ),
#             F.col("job_company").alias(
#                 "company"
#             ),
#             "overall_score",
#             "skill_score",
#             "experience_score",
#             "salary_score",
#             "location_score",
#             "language_score",
#         )
#     )
#     _step_log(batch_id, "15-high_priority-built", "lazy plan built (filters cached `scored`)")

#     # ============================================================
#     # 16. Publish high-priority matches
#     # ============================================================

#     _step_log(batch_id, "16-high_priority-isEmpty-BEGIN", "about to call isEmpty()")
#     with _Timer(metrics, "high_priority_check_sec"):
#         hp_is_empty = _df_is_empty(high_priority)
#     _step_log(batch_id, "16-high_priority-isEmpty-END", f"hp_is_empty={hp_is_empty}")

#     if not hp_is_empty:

#         # Small result set - cache it so the count below and the kafka
#         # write don't each re-filter `scored` from scratch.
#         high_priority = high_priority.cache()
#         persisted.append(high_priority)
#         metrics.high_priority_rows = high_priority.count()

#         (
#             high_priority
#             .select(
#                 F.col("application_id").alias("key"),
#                 F.to_json(
#                     F.struct("*")
#                 ).alias("value")
#             )
#             .write
#             .format("kafka")
#             .option(
#                 "kafka.bootstrap.servers",
#                 KAFKA_BOOTSTRAP
#             )
#             .option(
#                 "topic",
#                 "high_priority_matches"
#             )
#             .save()
#         )
#         _step_log(batch_id, "16-high_priority-write-END", "kafka save() returned successfully")

#         print(
#             f"[batch {batch_id}] "
#             f"High-priority matches published"
#         )

#     else:
#         metrics.high_priority_rows = 0
#         print(
#             f"[batch {batch_id}] "
#             f"No high-priority matches"
#         )

#     _step_log(batch_id, "17-done", "process_application_batch completed successfully")
#     print(
#         f"[batch {batch_id}] "
#         f"Batch completed successfully"
#     )
#     _finish("completed")

# # def process_application_batch(app_batch: DataFrame, batch_id: int,
# #                                 candidate_path: str, job_path: str,
# #                                 results_path: str, pending_path: str):
# #     spark = app_batch.sparkSession

# #     new_apps = app_batch.filter(F.col("op").isin("c", "r")).select(
# #         "application_id", "candidate_id", "post_id", "applied_date", "event_ts"
# #     )

# #     if _df_is_empty(new_apps):
# #         print(f"[batch {batch_id}] No new applications")
# #         return

# #     # Read pending applications
# #     if DeltaTable.isDeltaTable(spark, pending_path):
# #         pending = spark.read.format("delta").load(pending_path)
# #     else:
# #         pending_schema = new_apps.withColumn("retry_count", F.lit(0)).schema
# #         pending = spark.createDataFrame([], pending_schema)

# #     incoming = (
# #         new_apps.withColumn("retry_count", F.lit(0))
# #         .unionByName(pending, allowMissingColumns=True)
# #     )

# #     if _df_is_empty(incoming):
# #         print(f"[batch {batch_id}] No incoming applications")
# #         return

# #     # Read dimension tables, but only pull the columns the join/scoring
# #     # actually needs (plus name/job_title/company, which the high-priority
# #     # publish step needs downstream). Trimming columns here shrinks what
# #     # gets shuffled/broadcast and cached for the batch.
# #     CANDIDATE_JOIN_COLS = [
# #         "candidate_id", "name", "skills", "experience_range",
# #         "current_salary", "location", "nationality",
# #     ]
# #     JOB_JOIN_COLS = [
# #         "post_id", "company", "job_title", "skills_needed",
# #         "years_experience", "salary", "city", "country",
# #         "language", "work_mode",
# #     ]

# #     candidate_dim_raw = (
# #         spark.read.format("delta").load(candidate_path)
# #         .filter(F.col("is_active") == True)
# #         .select(*CANDIDATE_JOIN_COLS)
# #         .withColumn("_dim_present", F.lit(True))
# #         .localCheckpoint()
# #     )
# #     job_dim_raw = (
# #         spark.read.format("delta").load(job_path)
# #         .filter(F.col("status") != "closed")
# #         .select(*JOB_JOIN_COLS)
# #         .withColumn("_dim_present", F.lit(True))
# #         .localCheckpoint()
# #     )

# #     # Alias columns to avoid name collisions
# #     candidate_dim = _alias_dim_cols(candidate_dim_raw, "cand", ["candidate_id"])
# #     job_dim = _alias_dim_cols(job_dim_raw, "job", ["post_id"])

# #     enriched = (
# #         incoming
# #         .join(F.broadcast(candidate_dim), "candidate_id", "left")
# #         .join(F.broadcast(job_dim), "post_id", "left")
# #     )

# #     # FIX: Only mark as unresolved when the dimension RECORD is missing,
# #     # not when individual fields (e.g. skills) are null. Use a dedicated
# #     # `_dim_present` marker instead of a business column (e.g. name) so this
# #     # check stays correct even if a real field is legitimately null.
# #     resolved = enriched.filter(
# #         F.col("cand__dim_present").isNotNull() & F.col("job__dim_present").isNotNull()
# #     )
# #     unresolved = (
# #         enriched.filter(
# #             F.col("cand__dim_present").isNull() | F.col("job__dim_present").isNull()
# #         )
# #         .select("application_id", "candidate_id", "post_id",
# #                 "applied_date", "event_ts", "retry_count")
# #         .withColumn("retry_count", F.col("retry_count") + 1)
# #         .filter(F.col("retry_count") <= MAX_ENRICHMENT_RETRIES)
# #     )

# #     # Persist unresolved for next batch
# #     if not _df_is_empty(unresolved):
# #         unresolved.write.format("delta").mode("overwrite").save(pending_path)
# #         print(f"[batch {batch_id}] {unresolved.count()} apps pending retry")
# #     else:
# #         empty_pending = spark.createDataFrame([], unresolved.schema)
# #         empty_pending.write.format("delta").mode("overwrite").save(pending_path)

# #     if _df_is_empty(resolved):
# #         print(f"[batch {batch_id}] No resolved applications")
# #         candidate_dim.unpersist()
# #         job_dim.unpersist()
# #         return

# #     # Map aliased columns back to expected names for scoring UDFs
# #     scored = (
# #         resolved
# #         .withColumnRenamed("cand_skills", "skills")
# #         .withColumnRenamed("job_skills_needed", "skills_needed")
# #         .withColumnRenamed("cand_experience_range", "experience_range")
# #         .withColumnRenamed("job_years_experience", "years_experience")
# #         .withColumnRenamed("cand_current_salary", "current_salary")
# #         .withColumnRenamed("job_salary", "salary")
# #         .withColumnRenamed("cand_location", "location")
# #         .withColumnRenamed("job_city", "city")
# #         .withColumnRenamed("job_country", "country")
# #         .withColumnRenamed("job_work_mode", "work_mode")
# #         .withColumnRenamed("cand_nationality", "nationality")
# #         .withColumnRenamed("job_language", "language")
# #     )

# #     scored = score_applications(scored).cache()

# #     scored.write.format("delta").mode("append")         .option("mergeSchema", "true")         .save(results_path)
# #     print(f"[batch {batch_id}] Wrote {scored.count()} scored apps to results")

# #     high_priority = (
# #         scored.filter(F.col("priority") == "HIGH_PRIORITY")
# #         .select(
# #             "application_id", "candidate_id",
# #             F.col("cand_name").alias("candidate_name"),
# #             "post_id", F.col("job_job_title").alias("job_title"),
# #             F.col("job_company").alias("company"),
# #             "overall_score", "skill_score", "experience_score",
# #             "salary_score", "location_score", "language_score",
# #         )
# #         .cache()
# #     )

# #     if not _df_is_empty(high_priority):
# #         count = high_priority.count()
# #         (high_priority
# #          .select(F.col("application_id").alias("key"),
# #                  F.to_json(F.struct("*")).alias("value"))
# #          .write.format("kafka")
# #          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
# #          .option("topic", "high_priority_matches")
# #          .save())
# #         print(f"[batch {batch_id}] Published {count} high-priority matches")
# #     else:
# #         print(f"[batch {batch_id}] No high-priority matches")

# #     scored.unpersist()
# #     if not _df_is_empty(high_priority):
# #         high_priority.unpersist()
# #     candidate_dim.unpersist()
# #     job_dim.unpersist()


# def start_matching_query(spark: SparkSession, candidate_path: str, job_path: str):
#     results_path = f"{DELTA_ROOT}/match_results"
#     pending_path = f"{DELTA_ROOT}/pending_applications"

#     # Pre-create pending_applications as an empty Delta table up front,
#     # exactly like candidate_dim/job_dim below. This is pure JVM
#     # (addColumns + commit, no data write, no task scheduled), so it can't
#     # hit the Python-worker-spawn crash. It also means process_application_batch's
#     # `if DeltaTable.isDeltaTable(spark, pending_path):` check is always
#     # True from here on, so the crashing spark.createDataFrame([], schema)
#     # fallback never actually gets exercised in normal operation.
#     ensure_delta_table(spark, pending_path, PENDING_APPLICATIONS_SCHEMA, {})

#     # NOTE on startingOffsets: this only matters for the very first run of
#     # this query. Once `checkpointLocation` has a committed offset log,
#     # Structured Streaming always resumes from those committed offsets and
#     # ignores `startingOffsets` entirely - so there's no need (and no way)
#     # to make this option itself "checkpoint aware". Using 'earliest' here
#     # just controls where a brand-new checkpoint starts; every subsequent
#     # run picks up exactly where the checkpoint left off.
#     app_stream = read_cdc_stream(
#         spark, TOPICS["application"], APPLICATION_AFTER_SCHEMA,
#         ["applied_date", "last_updated"], starting_offsets="earliest"
#     )

#     query = (
#         app_stream.writeStream
#         .queryName("match_engine")
#         .foreachBatch(lambda df, bid: process_application_batch(
#             df, bid, candidate_path, job_path, results_path, pending_path
#         ))
#         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/match_engine")
#         .trigger(processingTime="30 seconds")
#         .start()
#     )
#     return query


# def main():
#     spark = build_spark()
#     spark.sparkContext.setLogLevel("WARN")

#     q_candidate, q_job, candidate_path, job_path = start_dimension_writers(spark)
#     q_match = start_matching_query(spark, candidate_path, job_path)

#     print("=" * 60)
#     print("Streaming queries started:")
#     print(f"  candidate_dim:  {q_candidate.name}")
#     print(f"  job_dim:        {q_job.name}")
#     print(f"  match_engine:   {q_match.name}")
#     print(f"  batch metrics:  {METRICS_LOG_PATH}")
#     print("=" * 60)
#     print("Waiting for data... Press Ctrl+C to stop.")
#     print()

#     try:
#         spark.streams.awaitAnyTermination()
#     except KeyboardInterrupt:
#         print("\nStopping...")
#         q_candidate.stop()
#         q_job.stop()
#         q_match.stop()
#         print("Done.")


# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
"""
Real-Time Candidate ↔ Job Matching Engine (Optimized)
"""

import os
import sys
from dotenv import load_dotenv

# Pin the worker Python to the exact interpreter running this script.
# On Windows especially, if PYSPARK_PYTHON is left unset, the driver and
# spawned Python workers can resolve to different `python.exe` installs
# (e.g. a spark-submit bundled one vs. a pip/venv one), which can crash
# workers with `EOFException` / "Python worker exited unexpectedly" even
# before any user code (UDFs, etc.) runs. Setting this before Spark starts
# removes that ambiguity. Must be set before SparkSession is created.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

load_dotenv()

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, BooleanType, ArrayType, TimestampType
)
from delta.tables import DeltaTable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "F:/DE_Projects/job_posting/project/checkpoint")
DELTA_ROOT = os.getenv("DELTA_ROOT", "F:/DE_Projects/job_posting/project/streaming_lake")

TOPICS = {
    "candidate": "candidate_events",
    "job": "job_events",
    "application": "application_events",
}

MATCH_WEIGHTS = {
    "skills": 0.30,
    "experience": 0.25,
    "salary": 0.15,
    "location": 0.20,
    "language": 0.10,
}

PRIORITY_THRESHOLDS = {
    "HIGH": 80,
    "MEDIUM": 65,
}

MAX_ENRICHMENT_RETRIES = 5

COUNTRY_LANGUAGES = {
    "united states": "english", "united kingdom": "english", "canada": "english",
    "australia": "english", "india": "english", "germany": "german",
    "france": "french", "spain": "spanish", "mexico": "spanish",
    "brazil": "portuguese", "portugal": "portuguese", "italy": "italian",
    "netherlands": "dutch", "sweden": "swedish", "poland": "polish",
    "egypt": "arabic", "saudi arabia": "arabic", "uae": "arabic",
    "japan": "japanese", "south korea": "korean", "china": "mandarin",
}


# Number of local worker threads/cores Spark will use. Defaults to 2.
# On Windows, each Spark task that needs a Python process (even a trivial
# write of an empty DataFrame) launches a brand-new python.exe subprocess -
# there's no fork(), unlike Linux/Mac. Using local[*] (all cores) means
# spawning that many processes essentially simultaneously, which is slow
# and easy for antivirus/Defender to disrupt mid-launch, producing random
# "Python worker exited unexpectedly (crashed)" / EOFException failures on
# whichever task lost the race - not tied to any particular row or code
# path. Lowering this reduces how many workers start at once. Override
# with SPARK_LOCAL_CORES if you want to dial it back up once things are
# stable.
SPARK_LOCAL_CORES = os.getenv("SPARK_LOCAL_CORES", "2")

# Every crash log so far has shown the SAME file failing to clean up
# afterward: org.xerial.snappy_snappy-java-*.jar - the native Snappy
# compression library, which Delta/Parquet writes use by default and which
# gets extracted to a temp .dll at runtime. That's a very common target for
# Windows Defender / AV real-time scanning to lock mid-extraction, which
# would explain crashes that persist even at local[2] (i.e. not actually a
# concurrency race - every write needs this same native lib).
# Set SPARK_PARQUET_CODEC=uncompressed (or "lz4", which has its own native
# lib but is a useful second data point) to test this without editing code.
SPARK_PARQUET_CODEC = os.getenv("SPARK_PARQUET_CODEC", "snappy")


# def build_spark() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("candidate-job-match-engine")
#         .master(f"local[{SPARK_LOCAL_CORES}]")
#         .config("spark.jars.packages",
#                 "io.delta:delta-spark_2.12:3.2.0,"
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog",
#                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#         .config("spark.sql.shuffle.partitions", "8")
#         .config("spark.sql.parquet.compression.codec", SPARK_PARQUET_CODEC)
#         # Reuse worker processes across tasks instead of spawning a fresh
#         # one per task where possible - fewer process launches, fewer
#         # chances for one to get killed mid-startup.
#         .config("spark.python.worker.reuse", "true")
#         # If a task's worker still gets killed transiently (e.g. AV), retry
#         # instead of failing the whole job outright.
#         .config("spark.task.maxFailures", "4")
#         .getOrCreate()
#     )
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("candidate-job-match-engine")
        .master(f"local[{SPARK_LOCAL_CORES}]")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        # FIXED: bypass Snappy native DLL entirely on Windows
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.python.worker.reuse", "true")
        .config("spark.task.maxFailures", "4")
        # Delta write optimization: on delta-spark 3.2.x the short-form
        # spark.databricks.delta.optimizeWrite.enabled/autoCompact.enabled
        # configs are known not to take effect (see delta-io/delta#3967) -
        # the properties.defaults.* form below is the confirmed workaround.
        # Verify it's actually doing something via files_written in your
        # own batch metrics rather than trusting it blindly.
        .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
        .config("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
        .getOrCreate()
    )

CANDIDATE_AFTER_SCHEMA = StructType([
    StructField("candidate_id", StringType()),
    StructField("name", StringType()),
    StructField("gender", StringType()),
    StructField("date_of_birth", IntegerType()),
    StructField("nationality", StringType()),
    StructField("location", StringType()),
    StructField("graduation_year", IntegerType()),
    StructField("gpa", FloatType()),
    StructField("education_level", StringType()),
    StructField("job_category", StringType()),
    StructField("current_title", StringType()),
    StructField("seniority", StringType()),
    StructField("experience_range", StringType()),
    StructField("current_company", StringType()),
    StructField("previous_companies", ArrayType(StringType())),
    StructField("previous_industries", ArrayType(StringType())),
    StructField("skills", ArrayType(StringType())),
    StructField("certifications", ArrayType(StringType())),
    StructField("current_salary", IntegerType()),
    StructField("is_active", BooleanType()),
    StructField("created_at", LongType()),
    StructField("updated_at", LongType()),
    StructField("version", IntegerType()),
])

JOB_AFTER_SCHEMA = StructType([
    StructField("post_id", StringType()),
    StructField("company", StringType()),
    StructField("category", StringType()),
    StructField("job_title", StringType()),
    StructField("application_method", StringType()),
    StructField("post_date", LongType()),
    StructField("deadline", LongType()),
    StructField("certifications", ArrayType(StringType())),
    StructField("skills_needed", ArrayType(StringType())),
    StructField("platform", StringType()),
    StructField("employment_type", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("language", StringType()),
    StructField("work_mode", StringType()),
    StructField("industry", StringType()),
    StructField("detailed_experience_level", StringType()),
    StructField("salary", IntegerType()),
    StructField("years_experience", StringType()),
    StructField("status", StringType()),
    StructField("lifecycle_stage", IntegerType()),
    StructField("created_at", LongType()),
    StructField("updated_at", LongType()),
    StructField("version", IntegerType()),
])

APPLICATION_AFTER_SCHEMA = StructType([
    StructField("application_id", StringType()),
    StructField("candidate_id", StringType()),
    StructField("post_id", StringType()),
    StructField("status", StringType()),
    StructField("lifecycle_stage", IntegerType()),
    StructField("applied_date", LongType()),
    StructField("last_updated", LongType()),
    StructField("final_outcome", StringType()),
    StructField("created_at", LongType()),
    StructField("updated_at", LongType()),
    StructField("version", IntegerType()),
])

# Schema of the `unresolved` DataFrame written to pending_applications in
# process_application_batch (step 10). Kept as an explicit top-level
# constant so it can be used to pre-create the Delta table via
# ensure_delta_table() (pure JVM, addColumns() only - no data written)
# instead of ever falling back to spark.createDataFrame([], schema) for a
# schema-only empty DataFrame. That fallback is what was crashing: even an
# empty Python list goes through sc.parallelize() -> PythonRDD, which still
# schedules one real task per default-parallelism partition, and each task
# still spawns a Python worker to do a (zero-row) handshake - which is
# exactly the worker that was dying on Windows. Pre-creating the table
# means that branch is never taken.
PENDING_APPLICATIONS_SCHEMA = StructType([
    StructField("application_id", StringType()),
    StructField("candidate_id", StringType()),
    StructField("post_id", StringType()),
    StructField("applied_date", TimestampType()),
    StructField("event_ts", TimestampType()),
    StructField("retry_count", IntegerType()),
])

# Schema of the `dead_letter` DataFrame written to dead_letter_applications
# in process_application_batch (step 10b). Same shape as
# PENDING_APPLICATIONS_SCHEMA plus reason/dead_lettered_at/dead_lettered_batch_id.
DEAD_LETTER_SCHEMA = StructType(
    PENDING_APPLICATIONS_SCHEMA.fields + [
        StructField("reason", StringType()),
        StructField("dead_lettered_at", TimestampType()),
        StructField("dead_lettered_batch_id", IntegerType()),
    ]
)


def envelope_schema(after_schema: StructType) -> StructType:
    return StructType([
        StructField("before", after_schema),
        StructField("after", after_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("source", StructType([
            StructField("table", StringType()),
            StructField("lsn", LongType()),
        ])),
    ])


def read_cdc_stream(spark: SparkSession, topic: str, after_schema: StructType,
                     ts_cols: list, starting_offsets: str = "earliest") -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), envelope_schema(after_schema)).alias("env")
    )

    df = parsed.select(
        F.col("env.op").alias("op"),
        (F.col("env.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
        F.col("env.after.*"),
    )

    for c in ts_cols:
        df = df.withColumn(c, (F.col(c) / 1_000_000).cast("timestamp"))

    return df.filter(F.col("op").isNotNull())

def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, extra_cols: dict):
    if DeltaTable.isDeltaTable(spark, path):
        return
    full_schema = StructType(schema.fields + [
        StructField(name, dtype, True) for name, dtype in extra_cols.items()
    ])
    # Create _delta_log only — no task, no Parquet file, no native DLL extraction
    DeltaTable.createIfNotExists(spark) \
        .addColumns(full_schema) \
        .location(path) \
        .execute()
# def ensure_delta_table(spark: SparkSession, path: str, schema: StructType, extra_cols: dict):
#     if DeltaTable.isDeltaTable(spark, path):
#         return
#     full_schema = StructType(schema.fields + [
#         StructField(name, dtype, True) for name, dtype in extra_cols.items()
#     ])
#     empty = spark.createDataFrame([], full_schema)
#     empty.write.format("delta").mode("overwrite").save(path)


def _df_is_empty(df: DataFrame) -> bool:
    return df.isEmpty()


def upsert_dim_batch(batch_df: DataFrame, batch_id: int, delta_path: str, key_col: str):
    if _df_is_empty(batch_df):
        print(f"[batch {batch_id}] No rows for {delta_path}")
        return

    spark = batch_df.sparkSession
    # Cache filtered subsets so they are computed once and reused across merge + count
    #
    # DEDUPE BEFORE MERGE:
    # A CDC micro-batch can contain multiple events for the same key
    # (created, then updated one or more times) - especially the very
    # first "earliest" batch, which can replay a key's entire history at
    # once. Delta's MERGE rejects multiple source rows matching the same
    # target row ([DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE]),
    # even with a `s.version >= t.version` condition on whenMatchedUpdateAll -
    # that condition can be true for several of those duplicate rows at
    # once, which is exactly the ambiguity Delta is refusing to resolve for
    # you. So collapse to one row per key (the highest `version`) before
    # ever building the merge.
    upserts_raw = batch_df.filter(F.col("op").isin("c", "r", "u")).drop("op", "event_ts")
    upserts = (
        upserts_raw
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(key_col).orderBy(F.col("version").desc())
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .cache()
    )
    # Deletes only carry the key column, so any duplicate delete events for
    # the same key are identical rows - dropDuplicates is enough here (no
    # "latest version wins" ordering needed since there's nothing to order by).
    deletes = batch_df.filter(F.col("op") == "d").select(key_col).dropDuplicates([key_col]).cache()

    target = DeltaTable.forPath(spark, delta_path)

    if not _df_is_empty(upserts):
        (target.alias("t")
         .merge(upserts.alias("s"), f"t.{key_col} = s.{key_col}")
         .whenMatchedUpdateAll(condition="s.version >= t.version")
         .whenNotMatchedInsertAll()
         .execute())
        print(f"[batch {batch_id}] Upserted {upserts.count()} rows into {delta_path}")

    if not _df_is_empty(deletes):
        (target.alias("t")
         .merge(deletes.alias("s"), f"t.{key_col} = s.{key_col}")
         .whenMatchedUpdate(set={"is_active": F.lit(False)})
         .execute())
        print(f"[batch {batch_id}] Soft-deleted {deletes.count()} rows in {delta_path}")

    upserts.unpersist()
    deletes.unpersist()


def start_dimension_writers(spark: SparkSession):
    candidate_path = f"{DELTA_ROOT}/candidate_dim"
    job_path = f"{DELTA_ROOT}/job_dim"

    ensure_delta_table(spark, candidate_path, CANDIDATE_AFTER_SCHEMA, {})
    ensure_delta_table(spark, job_path, JOB_AFTER_SCHEMA, {})

    # Same note as start_matching_query: 'earliest' only governs the
    # initial (no-checkpoint-yet) run; every later run resumes from
    # CHECKPOINT_ROOT/{candidate_dim,job_dim} automatically.
    candidate_stream = read_cdc_stream(
        spark, TOPICS["candidate"], CANDIDATE_AFTER_SCHEMA,
        ["created_at", "updated_at"], starting_offsets="earliest"
    )
    job_stream = read_cdc_stream(
        spark, TOPICS["job"], JOB_AFTER_SCHEMA,
        ["post_date", "deadline", "created_at", "updated_at"], starting_offsets="earliest"
    )

    q1 = (
        candidate_stream.writeStream
        .queryName("candidate_dim_writer")
        .foreachBatch(lambda df, bid: upsert_dim_batch(df, bid, candidate_path, "candidate_id"))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/candidate_dim")
        .trigger(processingTime="30 seconds")
        .start()
    )
    q2 = (
        job_stream.writeStream
        .queryName("job_dim_writer")
        .foreachBatch(lambda df, bid: upsert_dim_batch(df, bid, job_path, "post_id"))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/job_dim")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return q1, q2, candidate_path, job_path



# ---------------------------------------------------------------------------
# Native Spark scoring expressions (replaces the previous Python UDFs).
#
# Every UDF used to force a round-trip: JVM -> serialize -> Python worker ->
# deserialize -> back to JVM, for every single row. That serialization
# overhead (plus the Python worker process itself) was almost certainly the
# biggest cost in this job, and it's also the most likely explanation for
# the Python worker crashes you were seeing under load: UDF rows are
# processed by a separate Python process that Spark has much less control
# over (memory accounting, retries, etc.) than it does over native JVM code.
#
# Below, every score is expressed with built-in Spark SQL functions
# (split/lower/trim/array_intersect/size/regexp_extract_all/when/otherwise/
# element_at/transform/array_min/array_max) so everything stays inside the
# JVM and Catalyst can optimize/whole-stage-codegen it.
# ---------------------------------------------------------------------------

_NUM_RE = r"(\d+(?:\.\d+)?)"


def _norm_array(col_name: str):
    """Lowercase + trim every element of a string array column, treating
    null/empty arrays as empty (mirrors the old `have`/`need` set logic)."""
    return F.transform(F.coalesce(F.col(col_name), F.array()), lambda x: F.trim(F.lower(x)))


def skill_score_expr(candidate_skills_col: str, required_skills_col: str):
    need = _norm_array(required_skills_col)
    have = _norm_array(candidate_skills_col)
    overlap = F.size(F.array_intersect(have, need))
    return (
        F.when(F.size(need) == 0, F.lit(100.0))
        .when(F.size(have) == 0, F.lit(0.0))
        .otherwise(F.round(F.lit(100.0) * overlap / F.size(need), 1))
    )


def experience_score_expr(candidate_text_col: str, job_text_col: str):
    def bounds(text_col_name):
        nums = F.transform(
            F.regexp_extract_all(F.coalesce(F.col(text_col_name), F.lit("")), F.lit(_NUM_RE), 1),
            lambda x: x.cast("double"),
        )
        n = F.size(nums)
        has_plus = F.instr(F.coalesce(F.col(text_col_name), F.lit("")), "+") > 0
        lo = F.array_min(nums)
        hi = F.when((n == 1) & has_plus, F.array_min(nums) + F.lit(8.0)).otherwise(F.array_max(nums))
        valid = n > 0
        return valid, lo, hi

    c_valid, c_lo, c_hi = bounds(candidate_text_col)
    j_valid, j_lo, j_hi = bounds(job_text_col)

    c_mid = (c_lo + c_hi) / F.lit(2.0)
    gap = j_lo - c_mid
    over = c_mid - j_hi

    in_range_score = F.lit(100.0)
    below_score = F.greatest(F.lit(0.0), F.lit(100.0) - F.lit(20.0) * gap)
    above_score = F.greatest(F.lit(50.0), F.lit(100.0) - F.lit(8.0) * over)

    return (
        F.when(~(c_valid & j_valid), F.lit(50.0))
        .when((c_mid >= j_lo) & (c_mid <= j_hi), F.round(in_range_score, 1))
        .when(c_mid < j_lo, F.round(below_score, 1))
        .otherwise(F.round(above_score, 1))
    )


def salary_score_expr(candidate_salary_col: str, job_salary_col: str):
    cand = F.col(candidate_salary_col)
    job = F.col(job_salary_col)
    ratio = job / cand
    return (
        F.when(cand.isNull() | job.isNull() | (cand <= 0), F.lit(50.0))
        .when(ratio >= 1.15, F.lit(100.0))
        .when(ratio >= 1.0, F.lit(85.0))
        .when(ratio >= 0.90, F.lit(55.0))
        .otherwise(F.lit(20.0))
    )


def location_score_expr(candidate_location_col: str, job_city_col: str, job_country_col: str, work_mode_col: str):
    parts = F.split(F.trim(F.lower(F.coalesce(F.col(candidate_location_col), F.lit("")))), ",")
    cand_city = F.trim(F.element_at(parts, 1))
    cand_country = F.trim(F.element_at(parts, -1))
    jc = F.trim(F.lower(F.coalesce(F.col(job_city_col), F.lit(""))))
    jco = F.trim(F.lower(F.coalesce(F.col(job_country_col), F.lit(""))))
    is_remote = F.lower(F.trim(F.coalesce(F.col(work_mode_col), F.lit("")))) == "remote"
    has_location_data = F.col(candidate_location_col).isNotNull() & (
        F.col(job_city_col).isNotNull() | F.col(job_country_col).isNotNull()
    )

    return (
        F.when(is_remote, F.lit(100.0))
        .when(~has_location_data, F.lit(50.0))
        .when((cand_city != "") & (jc != "") & (cand_city == jc), F.lit(100.0))
        .when((cand_country != "") & (jco != "") & (cand_country == jco), F.lit(55.0))
        .otherwise(F.lit(15.0))
    )


def _country_lang_map():
    # Built lazily (not at module import time): F.lit()/F.create_map() need
    # an active SparkContext/JVM gateway, which only exists after
    # build_spark() has run. Building this at import time is what caused
    # `AssertionError` from pyspark's `_invoke_function` before any
    # SparkSession existed.
    return F.create_map(
        [F.lit(x) for pair in COUNTRY_LANGUAGES.items() for x in pair]
    )


def language_score_expr(nationality_col: str, job_language_col: str):
    job_lang = F.trim(F.lower(F.col(job_language_col)))
    inferred = F.element_at(
        _country_lang_map(),
        F.trim(F.lower(F.coalesce(F.col(nationality_col), F.lit("")))),
    )

    return (
        F.when(F.col(job_language_col).isNull(), F.lit(100.0))
        .when(inferred.isNull(), F.lit(50.0))
        .when(inferred == job_lang, F.lit(100.0))
        .when(job_lang == "english", F.lit(60.0))
        .otherwise(F.lit(25.0))
    )


def priority_expr(score_col: str):
    return (
        F.when(F.col(score_col).isNull(), F.lit("UNSCORED"))
        .when(F.col(score_col) >= PRIORITY_THRESHOLDS["HIGH"], F.lit("HIGH_PRIORITY"))
        .when(F.col(score_col) >= PRIORITY_THRESHOLDS["MEDIUM"], F.lit("MEDIUM_PRIORITY"))
        .otherwise(F.lit("LOW_PRIORITY"))
    )


def score_applications(joined: DataFrame) -> DataFrame:
    scored = (
        joined
        .withColumn("skill_score", skill_score_expr("skills", "skills_needed"))
        .withColumn("experience_score", experience_score_expr("experience_range", "years_experience"))
        .withColumn("salary_score", salary_score_expr("current_salary", "salary"))
        .withColumn("location_score", location_score_expr("location", "city", "country", "work_mode"))
        .withColumn("language_score", language_score_expr("nationality", "language"))
        .withColumn(
            "overall_score",
            F.round(
                F.col("skill_score") * MATCH_WEIGHTS["skills"]
                + F.col("experience_score") * MATCH_WEIGHTS["experience"]
                + F.col("salary_score") * MATCH_WEIGHTS["salary"]
                + F.col("location_score") * MATCH_WEIGHTS["location"]
                + F.col("language_score") * MATCH_WEIGHTS["language"],
                1,
            ),
        )
        .withColumn("priority", priority_expr("overall_score"))
        .withColumn("scored_at", F.current_timestamp())
    )
    return scored


import time
import datetime


def _step_log(batch_id: int, step: str, msg: str = ""):
    """Flush a timestamped line immediately so it lands in the console/log
    file even if the process crashes right after this step. Without
    flush=True, buffered stdout can be lost entirely when the JVM/worker
    dies, which is exactly what makes 'which step crashed' hard to tell
    from the raw stack trace alone."""
    ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] [batch {batch_id}] STEP {step}"
    if msg:
        line += f" -- {msg}"
    print(line, flush=True)
    sys.stdout.flush()


def _alias_dim_cols(df: DataFrame, prefix: str, exclude: list) -> DataFrame:
    """Rename all columns with a prefix, except excluded keys."""
    for col_name in df.columns:
        if col_name not in exclude:
            df = df.withColumnRenamed(col_name, f"{prefix}_{col_name}")
    return df

# ---------------------------------------------------------------------------
# Batch metrics
#
# Design goal: capture the metrics below WITHOUT adding new Spark actions
# beyond what the pipeline already needs. Every field here is filled in one
# of these ways:
#   - read off a DataFrame that's already cached/materialized by an action
#     that was going to run anyway (.count() on a cached df is a cache hit,
#     not a re-scan)
#   - read from Delta's own transaction history (operationMetrics), which
#     Delta computes and commits as part of every write/merge for free
#   - read from Catalyst's plan statistics (file/table size metadata),
#     which does not scan any data
#   - read from the local Spark UI's REST API (a plain HTTP call, not a
#     Spark action) for shuffle bytes
# The only genuinely new per-batch actions are the .count() calls on
# already-cached DataFrames, which are effectively free.
# ---------------------------------------------------------------------------

import json
import urllib.request
from dataclasses import dataclass, asdict
from typing import Optional

METRICS_LOG_PATH = os.getenv("METRICS_LOG_PATH", f"{DELTA_ROOT}/_metrics/batch_metrics.jsonl")


@dataclass
class BatchMetrics:
    batch_id: int
    batch_start_time: str = ""
    batch_end_time: str = ""
    batch_duration_sec: float = 0.0
    status: str = "running"

    incoming_rows: int = 0
    resolved_rows: int = 0
    unresolved_rows: int = 0
    dead_letter_rows: int = 0
    scored_rows: int = 0
    high_priority_rows: int = 0

    candidate_dim_total_rows: int = 0
    candidate_dim_updated_rows: int = 0

    job_dim_total_rows: int = 0
    job_dim_updated_rows: int = 0

    candidate_dim_broadcast_mb: float = 0.0
    job_dim_broadcast_mb: float = 0.0

    pending_write_sec: float = 0.0
    dead_letter_write_sec: float = 0.0
    resolved_check_sec: float = 0.0
    scoring_write_sec: float = 0.0
    high_priority_check_sec: float = 0.0

    files_read: int = 0
    files_written: int = 0

    shuffle_read_mb: float = 0.0
    shuffle_write_mb: float = 0.0

    input_rows_per_sec: float = 0.0

    def finalize(self, start_ts: float, end_ts: float):
        self.batch_duration_sec = round(end_ts - start_ts, 3)
        if self.batch_duration_sec > 0:
            self.input_rows_per_sec = round(self.incoming_rows / self.batch_duration_sec, 2)

    def log_line(self) -> str:
        return json.dumps(asdict(self), default=str)


class _Timer:
    """Context manager: records elapsed wall-clock seconds into
    metrics.<field_name>. Wrap exactly the code that performs the Spark
    action you want timed - nothing else."""

    def __init__(self, metrics: "BatchMetrics", field_name: str):
        self.metrics = metrics
        self.field_name = field_name
        self._start = None

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        setattr(self.metrics, self.field_name, round(time.time() - self._start, 3))
        return False


def _plan_size_mb(df: DataFrame) -> float:
    """Catalyst's estimated size of a DataFrame's plan, in MB. Reads size
    stats already tracked in table/file metadata - does not scan data.
    Best-effort: falls back to 0.0 if the internal API shape changes."""
    try:
        size_bytes = int(str(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))
        return round(size_bytes / (1024 * 1024), 3)
    except Exception:
        return 0.0


def _count_input_files(df: DataFrame) -> int:
    """Number of files backing this DataFrame's plan. Triggers file
    listing/metadata resolution only, never a data scan."""
    try:
        return len(df.inputFiles())
    except Exception:
        return 0


def _delta_last_operation_metrics(spark: SparkSession, path: str) -> dict:
    """operationMetrics from the most recent commit to a Delta table -
    Delta already computed these (numOutputRows, numOutputFiles,
    numTargetRowsUpdated, numTargetRowsInserted, ...) while performing the
    write/merge; this just reads the transaction log, no extra Spark job."""
    try:
        if not DeltaTable.isDeltaTable(spark, path):
            return {}
        rows = (
            DeltaTable.forPath(spark, path)
            .history(1)
            .select("operationMetrics")
            .collect()
        )
        if rows:
            return dict(rows[0]["operationMetrics"] or {})
    except Exception:
        pass
    return {}


def _m_int(metrics_dict: dict, *keys) -> Optional[int]:
    for k in keys:
        v = metrics_dict.get(k)
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return None


def _shuffle_metrics_since(spark: SparkSession, since_ms: float) -> tuple:
    """Best-effort shuffle read/write MB for stages completed after
    `since_ms`, pulled from the local Spark UI's REST API - a loopback HTTP
    call, not a Spark action. Returns (0.0, 0.0) if the UI is disabled
    (spark.ui.enabled=false) or unreachable, so this never breaks a batch."""
    try:
        ui_url = spark.sparkContext.uiWebUrl
        if not ui_url:
            return 0.0, 0.0
        app_id = spark.sparkContext.applicationId
        req_url = f"{ui_url}/api/v1/applications/{app_id}/stages?status=complete"
        with urllib.request.urlopen(req_url, timeout=2) as resp:
            stages = json.loads(resp.read())
        read_bytes = 0
        write_bytes = 0
        for st in stages:
            completion = st.get("completionTime")
            if not completion:
                continue
            completion_ms = datetime.datetime.strptime(
                completion.split(".")[0], "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
            if completion_ms >= since_ms:
                read_bytes += st.get("shuffleReadBytes", 0) or 0
                write_bytes += st.get("shuffleWriteBytes", 0) or 0
        return round(read_bytes / (1024 * 1024), 3), round(write_bytes / (1024 * 1024), 3)
    except Exception:
        return 0.0, 0.0


def _emit_batch_metrics(metrics: "BatchMetrics"):
    """Log the metrics record and append it as a JSON line to disk.
    Deliberately plain Python file I/O, NOT a Spark write: this file's own
    comments explain that even a single-row spark.createDataFrame(...) spins
    up a Python worker task, which is exactly the class of Windows/AV crash
    this pipeline was hardened against elsewhere. Metrics logging should
    never be able to take the streaming query down."""
    line = metrics.log_line()
    print(f"[batch {metrics.batch_id}] METRICS {line}", flush=True)
    try:
        os.makedirs(os.path.dirname(METRICS_LOG_PATH), exist_ok=True)
        with open(METRICS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"[batch {metrics.batch_id}] WARNING: failed to write metrics log: {e}", flush=True)


def process_application_batch(
    app_batch: DataFrame,
    batch_id: int,
    candidate_path: str,
    job_path: str,
    results_path: str,
    pending_path: str,
    dead_letter_path: str
):
    spark = app_batch.sparkSession
    batch_start_ts = time.time()
    batch_start_wall = datetime.datetime.now()
    metrics = BatchMetrics(batch_id=batch_id, batch_start_time=batch_start_wall.isoformat())

    # Everything we .cache()/.persist() goes here so it always gets
    # unpersisted, even on an early return - the old version leaked cached
    # dimension tables on every "no resolved applications" batch.
    persisted = []

    def _finish(status: str):
        metrics.status = status
        metrics.batch_end_time = datetime.datetime.now().isoformat()
        metrics.finalize(batch_start_ts, time.time())
        metrics.shuffle_read_mb, metrics.shuffle_write_mb = _shuffle_metrics_since(
            spark, batch_start_wall.timestamp() * 1000
        )
        for df in persisted:
            try:
                df.unpersist()
            except Exception:
                pass
        _emit_batch_metrics(metrics)

    _step_log(batch_id, "0-start", "process_application_batch entered")

    # ============================================================
    # 1. Get new applications
    # ============================================================

    new_apps = (
        app_batch
        .filter(F.col("op").isin("c", "r"))
        .select(
            "application_id",
            "candidate_id",
            "post_id",
            "applied_date",
            "event_ts"
        )
    )
    _step_log(batch_id, "1-new_apps-built", "lazy plan built (no action yet)")

    if _df_is_empty(new_apps):
        _step_log(batch_id, "1-new_apps-empty", "returning early")
        print(f"[batch {batch_id}] No new applications")
        _finish("no_new_apps")
        return
    _step_log(batch_id, "1-new_apps-checked", "not empty, continuing")

    # ============================================================
    # 2. Read pending applications
    # ============================================================

    pending_table_exists = DeltaTable.isDeltaTable(spark, pending_path)
    if pending_table_exists:
        pending = (
            spark.read
            .format("delta")
            .load(pending_path)
        )
    else:
        pending_schema = (
            new_apps
            .withColumn("retry_count", F.lit(0))
            .schema
        )
        pending = spark.createDataFrame(
            spark.sparkContext.emptyRDD(),
            pending_schema
        )
    metrics.files_read += _count_input_files(pending)
    _step_log(batch_id, "2-pending-read", f"pending source ready (existing_delta_table={pending_table_exists})")

    # ============================================================
    # 3. Combine new applications + pending applications
    #
    # OPTIMIZATION: cache `incoming` immediately. It's read twice below
    # (the isEmpty() check, then the join that builds `enriched`) - without
    # caching, both reads recompute the new_apps/pending union from scratch.
    # ============================================================

    incoming = (
        new_apps
        .withColumn("retry_count", F.lit(0))
        .unionByName(
            pending,
            allowMissingColumns=True
        )
        .cache()
    )
    persisted.append(incoming)
    _step_log(batch_id, "3-incoming-built", "lazy plan built, cached (reused by isEmpty() + join below)")

    if _df_is_empty(incoming):
        _step_log(batch_id, "3-incoming-empty", "returning early")
        print(f"[batch {batch_id}] No incoming applications")
        _finish("no_incoming")
        return
    _step_log(batch_id, "3-incoming-checked", "not empty, continuing")

    # `incoming` is already cached and materialized by isEmpty() above, so
    # this count is a cache hit, not a fresh scan of new_apps/pending.
    metrics.incoming_rows = incoming.count()

    # ============================================================
    # 4. Read candidate dimension
    # ============================================================

    CANDIDATE_JOIN_COLS = [
        "candidate_id",
        "name",
        "skills",
        "experience_range",
        "current_salary",
        "location",
        "nationality",
    ]

    candidate_dim_raw = (
        spark.read
        .format("delta")
        .load(candidate_path)
        .filter(F.col("is_active") == True)
        .select(*CANDIDATE_JOIN_COLS)
        .withColumn(
            "_dim_present",
            F.lit(True)
        )
    )
    metrics.files_read += _count_input_files(candidate_dim_raw)
    _step_log(batch_id, "4-candidate_dim-read", "lazy plan built")

    # ============================================================
    # 5. Read job dimension
    # ============================================================

    JOB_JOIN_COLS = [
        "post_id",
        "company",
        "job_title",
        "skills_needed",
        "years_experience",
        "salary",
        "city",
        "country",
        "language",
        "work_mode",
    ]

    job_dim_raw = (
        spark.read
        .format("delta")
        .load(job_path)
        .filter(F.col("status") != "closed")
        .select(*JOB_JOIN_COLS)
        .withColumn(
            "_dim_present",
            F.lit(True)
        )
    )
    metrics.files_read += _count_input_files(job_dim_raw)
    _step_log(batch_id, "5-job_dim-read", "lazy plan built")

    # ============================================================
    # 6. Alias + cache dimension columns
    #
    # OPTIMIZATION: cache both broadcast dimensions. They're materialized
    # once for the broadcast join; caching means the row-count metrics
    # below are cache hits instead of a second Delta scan.
    # ============================================================

    candidate_dim_aliased = _alias_dim_cols(
        candidate_dim_raw,
        "cand",
        ["candidate_id"]
    )
    job_dim_aliased = _alias_dim_cols(
        job_dim_raw,
        "job",
        ["post_id"]
    )
    # Catalyst's plan-stats size estimate, read from table metadata before
    # caching changes the plan shape - free, no data scan.
    metrics.candidate_dim_broadcast_mb = _plan_size_mb(candidate_dim_aliased)
    metrics.job_dim_broadcast_mb = _plan_size_mb(job_dim_aliased)

    candidate_dim = candidate_dim_aliased.cache()
    job_dim = job_dim_aliased.cache()
    persisted.extend([candidate_dim, job_dim])
    _step_log(batch_id, "6-aliased", "lazy plan built, dims cached (broadcast join reuses this)")

    # ============================================================
    # 7. Join applications with dimensions
    # ============================================================

    enriched = (
        incoming
        .join(
            F.broadcast(candidate_dim),
            "candidate_id",
            "left"
        )
        .join(
            F.broadcast(job_dim),
            "post_id",
            "left"
        )
    )
    _step_log(batch_id, "7-enriched-built", "lazy plan built (broadcast joins)")

    # ============================================================
    # 8. Identify resolved applications
    #
    # OPTIMIZATION (the biggest one from the log analysis): cache `resolved`
    # immediately. It was previously read twice - once for isEmpty(), once
    # again (via scored_input) to build `scored` - which recomputed the
    # entire join+filter lineage a second time. Caching here means the
    # isEmpty() check below pays the cost once, and scoring reuses it free.
    # ============================================================

    resolved = enriched.filter(
        F.col("cand__dim_present").isNotNull()
        &
        F.col("job__dim_present").isNotNull()
    ).cache()
    persisted.append(resolved)
    _step_log(batch_id, "8-resolved-built", "lazy plan built, cached to avoid recomputation at isEmpty()+scoring")

    # ============================================================
    # 9. Identify unresolved applications
    #
    # Rows that still can't be enriched AND have hit MAX_ENRICHMENT_RETRIES
    # are dead-lettered (written once, with a reason) instead of being
    # silently dropped - previously these just vanished with no record.
    # ============================================================

    still_unresolved = (
        enriched
        .filter(
            F.col("cand__dim_present").isNull()
            |
            F.col("job__dim_present").isNull()
        )
        .withColumn(
            "retry_count",
            F.col("retry_count") + 1
        )
    )

    unresolved = (
        still_unresolved
        .filter(F.col("retry_count") <= MAX_ENRICHMENT_RETRIES)
        .select(
            "application_id",
            "candidate_id",
            "post_id",
            "applied_date",
            "event_ts",
            "retry_count"
        )
    )
    _step_log(batch_id, "9-unresolved-built", "lazy plan built (no action yet)")

    dead_letter = (
        still_unresolved
        .filter(F.col("retry_count") > MAX_ENRICHMENT_RETRIES)
        .select(
            "application_id",
            "candidate_id",
            "post_id",
            "applied_date",
            "event_ts",
            "retry_count",
            F.when(F.col("cand__dim_present").isNull() & F.col("job__dim_present").isNull(),
                   F.lit("candidate_and_job_not_found"))
             .when(F.col("cand__dim_present").isNull(), F.lit("candidate_not_found"))
             .otherwise(F.lit("job_not_found"))
             .alias("reason"),
            F.current_timestamp().alias("dead_lettered_at"),
            F.lit(batch_id).alias("dead_lettered_batch_id")
        )
    )
    _step_log(batch_id, "9b-dead_letter-built", "lazy plan built (no action yet)")

    # ============================================================
    # 10. Persist unresolved applications
    #
    # IMPORTANT:
    # Do NOT cache unresolved.
    # Do NOT count unresolved.
    # The write itself is the Spark action.
    # Row/file counts for metrics come from Delta's own transaction history
    # below (operationMetrics), not from an extra .count() action.
    # ============================================================

    _step_log(batch_id, "10-pending-write-BEGIN", f"about to trigger the .save() action, table_exists={pending_table_exists}")

    with _Timer(metrics, "pending_write_sec"):
        (
            unresolved
            .write
            .format("delta")
            .mode("overwrite")
            .save(pending_path)
        )

    if pending_table_exists:
        _step_log(batch_id, "10-pending-write-END", "save() returned successfully (update path)")
        print(
            f"[batch {batch_id}] "
            f"Pending applications updated"
        )
    else:
        _step_log(batch_id, "10-pending-write-END", "save() returned successfully (create path)")
        print(
            f"[batch {batch_id}] "
            f"Created pending applications table"
        )

    pending_write_metrics = _delta_last_operation_metrics(spark, pending_path)
    metrics.unresolved_rows = _m_int(pending_write_metrics, "numOutputRows") or 0
    metrics.files_written += _m_int(pending_write_metrics, "numOutputFiles", "numFiles") or 0

    # dead_letter is append-only by design: each row is written exactly
    # once, at the moment it exceeds MAX_ENRICHMENT_RETRIES, and never
    # revisited afterward - unlike pending_applications, which must stay
    # overwrite since it represents the *current* full pending set each batch.
    _step_log(batch_id, "10b-dead_letter-write-BEGIN", "about to trigger the .save() action")

    with _Timer(metrics, "dead_letter_write_sec"):
        (
            dead_letter
            .write
            .format("delta")
            .mode("append")
            .save(dead_letter_path)
        )

    _step_log(batch_id, "10b-dead_letter-write-END", "save() returned successfully")

    dead_letter_write_metrics = _delta_last_operation_metrics(spark, dead_letter_path)
    metrics.dead_letter_rows = _m_int(dead_letter_write_metrics, "numOutputRows") or 0
    metrics.files_written += _m_int(dead_letter_write_metrics, "numOutputFiles", "numFiles") or 0

    # ============================================================
    # 11. Check resolved applications
    #
    # Keep this lightweight.
    # _df_is_empty() should use limit(1), not count().
    # `resolved` is cached above, so this no longer re-executes the
    # join/filter lineage.
    # ============================================================

    _step_log(batch_id, "11-resolved-isEmpty-BEGIN", "about to call isEmpty() on resolved (cached)")
    with _Timer(metrics, "resolved_check_sec"):
        resolved_is_empty = _df_is_empty(resolved)
    _step_log(batch_id, "11-resolved-isEmpty-END", f"resolved_is_empty={resolved_is_empty}")

    if resolved_is_empty:

        print(
            f"[batch {batch_id}] "
            f"No resolved applications"
        )
        _finish("no_resolved")
        return

    # `resolved`/`candidate_dim`/`job_dim` are all cached and already
    # materialized (by isEmpty()/the join above), so these counts are cache
    # hits, not fresh scans.
    metrics.resolved_rows = resolved.count()
    metrics.candidate_dim_total_rows = candidate_dim.count()
    metrics.job_dim_total_rows = job_dim.count()

    # candidate_dim/job_dim are upserted by separate concurrent streaming
    # queries (candidate_dim_writer/job_dim_writer), not by this batch. This
    # reads whatever the most recent MERGE into each table committed - a
    # useful proxy for "how much dimension churn is happening", but it isn't
    # guaranteed to be exactly this batch's own upsert.
    last_cand_merge = _delta_last_operation_metrics(spark, candidate_path)
    last_job_merge = _delta_last_operation_metrics(spark, job_path)
    metrics.candidate_dim_updated_rows = (
        (_m_int(last_cand_merge, "numTargetRowsUpdated") or 0)
        + (_m_int(last_cand_merge, "numTargetRowsInserted") or 0)
    )
    metrics.job_dim_updated_rows = (
        (_m_int(last_job_merge, "numTargetRowsUpdated") or 0)
        + (_m_int(last_job_merge, "numTargetRowsInserted") or 0)
    )

    # ============================================================
    # 12. Rename columns for scoring (reuses cached `resolved`)
    # ============================================================

    scored_input = (
        resolved
        .withColumnRenamed(
            "cand_skills",
            "skills"
        )
        .withColumnRenamed(
            "job_skills_needed",
            "skills_needed"
        )
        .withColumnRenamed(
            "cand_experience_range",
            "experience_range"
        )
        .withColumnRenamed(
            "job_years_experience",
            "years_experience"
        )
        .withColumnRenamed(
            "cand_current_salary",
            "current_salary"
        )
        .withColumnRenamed(
            "job_salary",
            "salary"
        )
        .withColumnRenamed(
            "cand_location",
            "location"
        )
        .withColumnRenamed(
            "job_city",
            "city"
        )
        .withColumnRenamed(
            "job_country",
            "country"
        )
        .withColumnRenamed(
            "job_work_mode",
            "work_mode"
        )
        .withColumnRenamed(
            "cand_nationality",
            "nationality"
        )
        .withColumnRenamed(
            "job_language",
            "language"
        )
    )
    _step_log(batch_id, "12-scored_input-built", "lazy plan built (reuses cached `resolved`)")

    # ============================================================
    # 13. Score applications
    #
    # OPTIMIZATION: cache `scored` immediately. It was previously read
    # twice - once for the results write, once again (filtered) for
    # high_priority.isEmpty() - which recomputed the whole scoring
    # transformation a second time. This is the same class of bug as #8,
    # and is the most likely explanation for the ~3 sec high_priority
    # isEmpty() cost even when the result was empty.
    # ============================================================

    scored = score_applications(scored_input).cache()
    persisted.append(scored)
    _step_log(batch_id, "13-scored-built", "lazy plan built, cached to avoid recomputation at high_priority isEmpty()")

    # ============================================================
    # 14. Write results
    # ============================================================

    _step_log(batch_id, "14-results-write-BEGIN", "about to trigger the .save() action")

    with _Timer(metrics, "scoring_write_sec"):
        (
            scored
            .write
            .format("delta")
            .mode("append")
            .option(
                "mergeSchema",
                "true"
            )
            .save(results_path)
        )
    _step_log(batch_id, "14-results-write-END", "save() returned successfully")

    print(
        f"[batch {batch_id}] "
        f"Scored applications written to results"
    )

    results_write_metrics = _delta_last_operation_metrics(spark, results_path)
    # Prefer Delta's own commit metrics; fall back to counting the cached
    # `scored` DataFrame (cheap - it's already materialized) only if Delta
    # didn't report a row count.
    metrics.scored_rows = _m_int(results_write_metrics, "numOutputRows")
    if metrics.scored_rows is None:
        metrics.scored_rows = scored.count()
    metrics.files_written += _m_int(results_write_metrics, "numOutputFiles", "numFiles") or 0

    # ============================================================
    # 15. Find high-priority matches (filters the cached `scored`)
    # ============================================================

    high_priority = (
        scored
        .filter(
            F.col("priority") == "HIGH_PRIORITY"
        )
        .select(
            "application_id",
            "candidate_id",
            F.col("cand_name").alias(
                "candidate_name"
            ),
            "post_id",
            F.col("job_job_title").alias(
                "job_title"
            ),
            F.col("job_company").alias(
                "company"
            ),
            "overall_score",
            "skill_score",
            "experience_score",
            "salary_score",
            "location_score",
            "language_score",
        )
    )
    _step_log(batch_id, "15-high_priority-built", "lazy plan built (filters cached `scored`)")

    # ============================================================
    # 16. Publish high-priority matches
    # ============================================================

    _step_log(batch_id, "16-high_priority-isEmpty-BEGIN", "about to call isEmpty()")
    with _Timer(metrics, "high_priority_check_sec"):
        hp_is_empty = _df_is_empty(high_priority)
    _step_log(batch_id, "16-high_priority-isEmpty-END", f"hp_is_empty={hp_is_empty}")

    if not hp_is_empty:

        # Small result set - cache it so the count below and the kafka
        # write don't each re-filter `scored` from scratch.
        high_priority = high_priority.cache()
        persisted.append(high_priority)
        metrics.high_priority_rows = high_priority.count()

        (
            high_priority
            .select(
                F.col("application_id").alias("key"),
                F.to_json(
                    F.struct("*")
                ).alias("value")
            )
            .write
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP
            )
            .option(
                "topic",
                "high_priority_matches"
            )
            .save()
        )
        _step_log(batch_id, "16-high_priority-write-END", "kafka save() returned successfully")

        print(
            f"[batch {batch_id}] "
            f"High-priority matches published"
        )

    else:
        metrics.high_priority_rows = 0
        print(
            f"[batch {batch_id}] "
            f"No high-priority matches"
        )

    _step_log(batch_id, "17-done", "process_application_batch completed successfully")
    print(
        f"[batch {batch_id}] "
        f"Batch completed successfully"
    )
    _finish("completed")

# def process_application_batch(app_batch: DataFrame, batch_id: int,
#                                 candidate_path: str, job_path: str,
#                                 results_path: str, pending_path: str):
#     spark = app_batch.sparkSession

#     new_apps = app_batch.filter(F.col("op").isin("c", "r")).select(
#         "application_id", "candidate_id", "post_id", "applied_date", "event_ts"
#     )

#     if _df_is_empty(new_apps):
#         print(f"[batch {batch_id}] No new applications")
#         return

#     # Read pending applications
#     if DeltaTable.isDeltaTable(spark, pending_path):
#         pending = spark.read.format("delta").load(pending_path)
#     else:
#         pending_schema = new_apps.withColumn("retry_count", F.lit(0)).schema
#         pending = spark.createDataFrame([], pending_schema)

#     incoming = (
#         new_apps.withColumn("retry_count", F.lit(0))
#         .unionByName(pending, allowMissingColumns=True)
#     )

#     if _df_is_empty(incoming):
#         print(f"[batch {batch_id}] No incoming applications")
#         return

#     # Read dimension tables, but only pull the columns the join/scoring
#     # actually needs (plus name/job_title/company, which the high-priority
#     # publish step needs downstream). Trimming columns here shrinks what
#     # gets shuffled/broadcast and cached for the batch.
#     CANDIDATE_JOIN_COLS = [
#         "candidate_id", "name", "skills", "experience_range",
#         "current_salary", "location", "nationality",
#     ]
#     JOB_JOIN_COLS = [
#         "post_id", "company", "job_title", "skills_needed",
#         "years_experience", "salary", "city", "country",
#         "language", "work_mode",
#     ]

#     candidate_dim_raw = (
#         spark.read.format("delta").load(candidate_path)
#         .filter(F.col("is_active") == True)
#         .select(*CANDIDATE_JOIN_COLS)
#         .withColumn("_dim_present", F.lit(True))
#         .localCheckpoint()
#     )
#     job_dim_raw = (
#         spark.read.format("delta").load(job_path)
#         .filter(F.col("status") != "closed")
#         .select(*JOB_JOIN_COLS)
#         .withColumn("_dim_present", F.lit(True))
#         .localCheckpoint()
#     )

#     # Alias columns to avoid name collisions
#     candidate_dim = _alias_dim_cols(candidate_dim_raw, "cand", ["candidate_id"])
#     job_dim = _alias_dim_cols(job_dim_raw, "job", ["post_id"])

#     enriched = (
#         incoming
#         .join(F.broadcast(candidate_dim), "candidate_id", "left")
#         .join(F.broadcast(job_dim), "post_id", "left")
#     )

#     # FIX: Only mark as unresolved when the dimension RECORD is missing,
#     # not when individual fields (e.g. skills) are null. Use a dedicated
#     # `_dim_present` marker instead of a business column (e.g. name) so this
#     # check stays correct even if a real field is legitimately null.
#     resolved = enriched.filter(
#         F.col("cand__dim_present").isNotNull() & F.col("job__dim_present").isNotNull()
#     )
#     unresolved = (
#         enriched.filter(
#             F.col("cand__dim_present").isNull() | F.col("job__dim_present").isNull()
#         )
#         .select("application_id", "candidate_id", "post_id",
#                 "applied_date", "event_ts", "retry_count")
#         .withColumn("retry_count", F.col("retry_count") + 1)
#         .filter(F.col("retry_count") <= MAX_ENRICHMENT_RETRIES)
#     )

#     # Persist unresolved for next batch
#     if not _df_is_empty(unresolved):
#         unresolved.write.format("delta").mode("overwrite").save(pending_path)
#         print(f"[batch {batch_id}] {unresolved.count()} apps pending retry")
#     else:
#         empty_pending = spark.createDataFrame([], unresolved.schema)
#         empty_pending.write.format("delta").mode("overwrite").save(pending_path)

#     if _df_is_empty(resolved):
#         print(f"[batch {batch_id}] No resolved applications")
#         candidate_dim.unpersist()
#         job_dim.unpersist()
#         return

#     # Map aliased columns back to expected names for scoring UDFs
#     scored = (
#         resolved
#         .withColumnRenamed("cand_skills", "skills")
#         .withColumnRenamed("job_skills_needed", "skills_needed")
#         .withColumnRenamed("cand_experience_range", "experience_range")
#         .withColumnRenamed("job_years_experience", "years_experience")
#         .withColumnRenamed("cand_current_salary", "current_salary")
#         .withColumnRenamed("job_salary", "salary")
#         .withColumnRenamed("cand_location", "location")
#         .withColumnRenamed("job_city", "city")
#         .withColumnRenamed("job_country", "country")
#         .withColumnRenamed("job_work_mode", "work_mode")
#         .withColumnRenamed("cand_nationality", "nationality")
#         .withColumnRenamed("job_language", "language")
#     )

#     scored = score_applications(scored).cache()

#     scored.write.format("delta").mode("append")         .option("mergeSchema", "true")         .save(results_path)
#     print(f"[batch {batch_id}] Wrote {scored.count()} scored apps to results")

#     high_priority = (
#         scored.filter(F.col("priority") == "HIGH_PRIORITY")
#         .select(
#             "application_id", "candidate_id",
#             F.col("cand_name").alias("candidate_name"),
#             "post_id", F.col("job_job_title").alias("job_title"),
#             F.col("job_company").alias("company"),
#             "overall_score", "skill_score", "experience_score",
#             "salary_score", "location_score", "language_score",
#         )
#         .cache()
#     )

#     if not _df_is_empty(high_priority):
#         count = high_priority.count()
#         (high_priority
#          .select(F.col("application_id").alias("key"),
#                  F.to_json(F.struct("*")).alias("value"))
#          .write.format("kafka")
#          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
#          .option("topic", "high_priority_matches")
#          .save())
#         print(f"[batch {batch_id}] Published {count} high-priority matches")
#     else:
#         print(f"[batch {batch_id}] No high-priority matches")

#     scored.unpersist()
#     if not _df_is_empty(high_priority):
#         high_priority.unpersist()
#     candidate_dim.unpersist()
#     job_dim.unpersist()


def start_matching_query(spark: SparkSession, candidate_path: str, job_path: str):
    results_path = f"{DELTA_ROOT}/match_results"
    pending_path = f"{DELTA_ROOT}/pending_applications"
    dead_letter_path = f"{DELTA_ROOT}/dead_letter_applications"

    # Pre-create pending_applications as an empty Delta table up front,
    # exactly like candidate_dim/job_dim below. This is pure JVM
    # (addColumns + commit, no data write, no task scheduled), so it can't
    # hit the Python-worker-spawn crash. It also means process_application_batch's
    # `if DeltaTable.isDeltaTable(spark, pending_path):` check is always
    # True from here on, so the crashing spark.createDataFrame([], schema)
    # fallback never actually gets exercised in normal operation.
    ensure_delta_table(spark, pending_path, PENDING_APPLICATIONS_SCHEMA, {})

    # Pre-create dead_letter_applications the same way, for the same reason.
    ensure_delta_table(spark, dead_letter_path, DEAD_LETTER_SCHEMA, {})

    # NOTE on startingOffsets: this only matters for the very first run of
    # this query. Once `checkpointLocation` has a committed offset log,
    # Structured Streaming always resumes from those committed offsets and
    # ignores `startingOffsets` entirely - so there's no need (and no way)
    # to make this option itself "checkpoint aware". Using 'earliest' here
    # just controls where a brand-new checkpoint starts; every subsequent
    # run picks up exactly where the checkpoint left off.
    app_stream = read_cdc_stream(
        spark, TOPICS["application"], APPLICATION_AFTER_SCHEMA,
        ["applied_date", "last_updated"], starting_offsets="earliest"
    )

    query = (
        app_stream.writeStream
        .queryName("match_engine")
        .foreachBatch(lambda df, bid: process_application_batch(
            df, bid, candidate_path, job_path, results_path, pending_path, dead_letter_path
        ))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/match_engine")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    q_candidate, q_job, candidate_path, job_path = start_dimension_writers(spark)
    q_match = start_matching_query(spark, candidate_path, job_path)

    print("=" * 60)
    print("Streaming queries started:")
    print(f"  candidate_dim:  {q_candidate.name}")
    print(f"  job_dim:        {q_job.name}")
    print(f"  match_engine:   {q_match.name}")
    print(f"  batch metrics:  {METRICS_LOG_PATH}")
    print("=" * 60)
    print("Waiting for data... Press Ctrl+C to stop.")
    print()

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
        q_candidate.stop()
        q_job.stop()
        q_match.stop()
        print("Done.")


if __name__ == "__main__":
    main()
#  spark-submit --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3" match_engine.py    