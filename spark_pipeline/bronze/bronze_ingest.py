"""
Bronze layer — raw landing zone.
================================
Contract: bronze is a verbatim, append-only copy of what Debezium put on
Kafka. No parsing of before/after fields, no dropping columns, no
normalization, no validation. If silver/gold logic ever has a bug, bronze
is the replay source of truth.

Batch, not streaming: each run reads a BOUNDED offset range per partition
(startingOffsets = last bookmark, endingOffsets = latest at run start),
so it is safe to trigger from Airflow/cron on a schedule. Offsets consumed
are recorded in a plain JSON bookmark file (see _write_offset_bookmark) ->
reruns are idempotent (Kafka offsets are exactly-once addressable, unlike
message content).

NOTE: offset bookmarks are deliberately stored as a local JSON file, NOT a
Delta table. Writing them via spark.createDataFrame(python_list, ...)
would hand Spark a raw Python object to serialize, which spins up a
PySpark worker process over a local socket -- on some Windows setups
(commonly antivirus/Defender intercepting that socket) that worker gets
killed and the job crashes with a cryptic EOFException. The rest of this
pipeline never needs a Python worker (Kafka reads + native Spark SQL
functions run entirely in the JVM), so avoiding it here keeps the whole
job free of that failure mode.
"""

import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS, ENTITY_TOPICS, BRONZE_PATHS,
    CONTROL_ROOT, MAX_OFFSETS_PER_TRIGGER,
)
from common.schemas import ENVELOPE_SCHEMAS


def _bookmark_path(topic: str) -> str:
    return os.path.join(CONTROL_ROOT, f"{topic}_offsets.json")


def _read_offset_bookmark(topic: str) -> str:
    """Return a startingOffsets JSON string: bookmark if we have one, else 'earliest'."""
    path = _bookmark_path(topic)
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)["ending_offsets_json"]
    return "earliest"


def _write_offset_bookmark(topic: str, ending_offsets_json: str, rows_read: int):
    os.makedirs(CONTROL_ROOT, exist_ok=True)
    with open(_bookmark_path(topic), "w") as f:
        json.dump({
            "topic": topic,
            "ending_offsets_json": ending_offsets_json,
            "rows_read": rows_read,
            "run_ts": datetime.now(timezone.utc).isoformat(),
        }, f)


def ingest_entity(spark: SparkSession, entity: str) -> int:
    """Bounded batch read of one topic; append raw envelopes to bronze. Returns row count."""
    topic = ENTITY_TOPICS[entity]
    starting = _read_offset_bookmark(topic)

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("endingOffsets", "latest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    if raw.isEmpty():  # native check, no .rdd conversion -> no Python worker involved
        return 0

    # CACHE HERE: without this, `raw` gets re-read from Kafka (network round
    # trip + JSON parsing) separately for isEmpty(), the bronze write, the
    # offset groupBy below, AND the row count -- up to 4x the real work for
    # zero benefit. Caching materializes it once and reuses that.
    raw = raw.cache()

    envelope_schema = ENVELOPE_SCHEMAS[entity]

    bronze = (
        raw.select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("value").cast("string").alias("_raw_value"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("_envelope", F.from_json(F.col("_raw_value"), envelope_schema))
        # true global dedup key: a Kafka offset is unique per (topic,partition)
        # for all time, unlike entity_id+version which can legitimately repeat
        # after a replay or an out-of-order redelivery.
        .withColumn(
            "_event_id",
            F.sha2(F.concat_ws("|", "_kafka_topic", "_kafka_partition", "_kafka_offset"), 256),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
        .select(
            "_event_id", "_envelope.*",
            "_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset",
            "_kafka_timestamp", "_ingested_at", "_ingest_date", "_raw_value",
        )
        .cache()  # reused by the write below AND the final count()
    )

    (
        bronze.write.format("delta")
        .mode("append")
        .partitionBy("_ingest_date", "op")
        .option("mergeSchema", "true")
        .save(BRONZE_PATHS[entity])
    )

    # bookmark = max offset per partition actually read, so the next run
    # starts exactly where this one left off.
    ending = (
        raw.groupBy("partition").agg(F.max("offset").alias("max_offset"))
        .collect()
    )
    ending_json = json.dumps({
        topic: {str(r["partition"]): r["max_offset"] + 1 for r in ending}
    })
    rows_read = bronze.count()
    _write_offset_bookmark(topic, ending_json, rows_read)
    raw.unpersist()
    bronze.unpersist()
    return rows_read


def run_bronze(spark: SparkSession) -> dict:
    return {entity: ingest_entity(spark, entity) for entity in ENTITY_TOPICS}
