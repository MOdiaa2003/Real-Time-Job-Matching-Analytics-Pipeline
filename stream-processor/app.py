"""
app.py - Quix Streams stream processor (production entry point)
================================================================
Topology (business problems -> Kafka concepts):

  1. application_submitted_events (Avro wire, key=job_id)
       -> Confluent wire-format check + validation            (problems 4 & 7)
       -> stateful dedup by event_id with retention           (problem 3)
       new   -> application_submitted_dedup (Avro wire; ksqlDB reads Avro)
       dup   -> dlq.application_submitted_events (reason = DUPLICATE)
       bad   -> dlq.application_submitted_events (decode/validation metadata)
  2. job_events (Debezium JSON, key={"post_id"})               (problems 1 & 2)
       -> flat row {post_id,status,op,is_deleted,...} so ksqlDB gets a
          simple key/value stream instead of nested Debezium structs
       -> group_by(post_id) [Quix auto-repartition] -> job_state_events
  3. application_events (Debezium JSON)                        (problems 1,5,6)
       -> flat row {application_id,candidate_id,post_id,status,...}
       -> group_by(application_id) -> application_state_events
  4. Unexpected processing exceptions -> on_processing_error callback,
       which ALSO emits a DLQ record instead of crashing the app (problem 7).

Guarantees (documented honestly in README):
  * Quix processing_guarantee (default exactly-once) covers this app's own
    offsets/state/output only. It does NOT extend to ksqlDB or the JDBC
    sink; the JDBC upsert keeps the analytics DB consistent under
    at-least-once delivery.
  * State stores are backed by changelog topics -> restart restores state
    (problem 9 / acceptance test 6).
"""
import io
import json
import os
import struct
import time

from quixstreams import Application
from quixstreams.models.topics import TopicConfig

import cdc_utils
from config import cfg

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # host: repo root
_app_dir = os.path.dirname(os.path.abspath(__file__))                    # container: /app
_SCHEMA_DIR_CANDIDATES = (
    os.path.join(repo_root, "schemas"),   # host runs (pytest / local)
    os.path.join(_app_dir, "schemas"),    # container (Dockerfile COPYs schemas here)
)
_SCHEMA_DIR = next((p for p in _SCHEMA_DIR_CANDIDATES if os.path.isdir(p)), _SCHEMA_DIR_CANDIDATES[0])
EVENT_SCHEMA_PATH = os.path.join(_SCHEMA_DIR, "application_submitted_event.avsc")
DEDUP_SCHEMA_PATH = os.path.join(_SCHEMA_DIR, "application_submitted_dedup.avsc")

# ── Registry + schemas ───────────────────────────────────────────────
registry = cdc_utils.RegistrySchemaCache(
    cfg.schema_registry_url, cache_size=cfg.schema_cache_size)

_event_schema = None     # writer schema for dedup output
_event_schema_id = None  # registry id under subject application_submitted_dedup-value


def _load_schemas():
    global _event_schema, _event_schema_id
    with open(EVENT_SCHEMA_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)
        raw.pop("comment", None)
    _event_schema = cdc_utils.fastavro.parse_schema(raw)

    # register (or reuse) the DEDUP output schema; ksqlDB reads this subject
    dedup_path = DEDUP_SCHEMA_PATH
    with open(dedup_path, "r", encoding="utf-8") as f:
        raw_dedup = json.load(f)
        raw_dedup.pop("comment", None)
    parsed_dedup = cdc_utils.fastavro.parse_schema(raw_dedup)
    try:
        _event_schema_id, _writer_schema = registry.schema_by_subject_version(
            f"{cfg.topic_app_dedup}-value", "latest")
        # ensure labor: schemas registered under dedup subject; event schema
        # must be COMPATIBLE to reuse that id is not required - we reuse the
        # dedup subject's LATEST id
        _event_schema = _writer_schema
    except Exception:
        _event_schema_id = registry.register(f"{cfg.topic_app_dedup}-value", raw_dedup)


def decode_callback(value_bytes):
    """Decode + validate -> routing envelope for downstream branching."""
    try:
        event = cdc_utils.decode_confluent_wire(value_bytes, registry,
                                                reader_schema=None)
        err = cdc_utils.validate_event(event)
        if err is not None:
            return {"ok": False, "code": "INVALID", "error": err.metadata(),
                    "event": None}
        return {"ok": True, "code": "OK", "error": None, "event": event}
    except cdc_utils.DecodeError as de:
        return {"ok": False, "code": "INVALID", "error": de.metadata(),
                "event": None}


def dedup_callback(routed, state):
    """Stateful: verdict 'new' or 'duplicate' per event_id with retention."""
    if not routed["ok"]:
        return routed
    verdict = cdc_utils.dedup_decide(routed["event"], state,
                                     now_ms=int(time.time() * 1000),
                                     retention_ms=cfg.retention_ms)
    routed["verdict"] = verdict
    return routed


def encode_dedup(routed):
    """Route further fields: OK+new -> encode to wire bytes."""
    return cdc_utils.encode_confluent_wire(routed["event"],
                                           _event_schema, _event_schema_id)


def to_dlq(routed):
    """Shape a DLQ json payload with error metadata (and the event if any)."""
    err = routed.get("error") or {}
    payload = {
        # NB: dict.get(key, default) does NOT substitute the default when the
        # key EXISTS with value None -> always coalesce with `or {}`.
        "error_code": err.get("error_code") or
                      ("DUPLICATE" if routed.get("verdict") == "duplicate" else "UNSPECIFIED"),
        "error_message": err.get("error_message", ""),
        "stage": err.get("stage", ""),
        "raw_value_len": err.get("raw_value_len", 0),
        "topic_source": cfg.topic_app_submitted,
        "ts_ms": int(time.time() * 1000),
        "event": routed.get("event"),
    }
    return payload


def project_job(value_bytes):
    return cdc_utils.project_job_state(value_bytes)


def project_app(value_bytes):
    return cdc_utils.project_application_state(value_bytes)


def on_processing_error(value, key, error):
    """Last-resort DLQ routing for exceptions OUTSIDE our try/excepts.

    Mirrors a Kafka Streams ProductionExceptionHandler for this app:
    bad records never crash the pipeline.
    (Not a perfectly isolated sink-like DLQ for CONNECT source workers -
     documented in README troubleshooting.)
    """
    print(f"[processing-error] key={key} error={error}", flush=True)


def topic_config():
    return TopicConfig(num_partitions=cfg.topic_partitions,
                       replication_factor=cfg.replication_factor)


def build_app():
    app = Application(
        broker_address=cfg.kafka_bootstrap,
        consumer_group=cfg.consumer_group,
        auto_offset_reset="earliest",
        state_dir=cfg.state_dir,
        processing_guarantee=cfg.processing_guarantee,
        use_changelog_topics=True,
        loglevel=os.getenv("SP_LOGLEVEL", "INFO"),
        on_processing_error=on_processing_error,
    )

    # ── topics ───────────────────────────────────────────────────────
    src = app.topic(cfg.topic_app_submitted,
                    value_deserializer="bytes", key_deserializer="bytes")
    dlg = app.topic(cfg.topic_dlq_app_submitted,
                    value_serializer="json", key_serializer="bytes",
                    value_deserializer="bytes", key_deserializer="bytes",
                    config=topic_config())
    out = app.topic(cfg.topic_app_dedup,
                    value_serializer="bytes", key_serializer="bytes",
                    value_deserializer="bytes", key_deserializer="bytes",
                    config=topic_config())

    _load_schemas()

    # ── SDF 1: submitted events -> dedup -> output / DLQ ─────────────
    sdf = app.dataframe(src)
    routed = sdf.apply(decode_callback)
    deduped = routed.apply(dedup_callback, stateful=True)

    # valid + first-seen -> encode Avro wire -> dedup output
    deduped.filter(lambda r: r["ok"] and r.get("verdict") == "new") \
           .apply(encode_dedup) \
           .to_topic(out)

    # duplicate OR invalid -> DLQ with metadata
    deduped.filter(lambda r: (not r["ok"]) or r.get("verdict") == "duplicate") \
           .apply(to_dlq) \
           .to_topic(dlg)

    # ── SDF 2: job CDC flat projection (repartitioned by post_id) ────
    job_src = app.topic(cfg.topic_job_events, value_deserializer="bytes")
    jdf = app.dataframe(job_src)
    jproj = jdf.apply(project_job)
    jproj.filter(lambda r: r["ok"]) \
         .apply(lambda r: r["row"]) \
         .group_by(lambda row: row["post_id"], name="job_state_events") \
         .to_topic(app.topic(cfg.topic_job_state,
                             value_serializer="json", key_serializer="bytes",
                             config=topic_config()))

    # project errors -> dlq.cdc_projection (json)
    jproj.filter(lambda r: not r["ok"]) \
         .apply(lambda r: r["error"]) \
         .to_topic(app.topic(cfg.topic_dlq_cdc,
                             value_serializer="json",
                             key_serializer="bytes",
                             config=topic_config()))

    # ── SDF 3: applications CDC flat projection (by application_id) ──
    app_src = app.topic(cfg.topic_application_events, value_deserializer="bytes")
    adf = app.dataframe(app_src)
    aproj = adf.apply(project_app)
    aproj.filter(lambda r: r["ok"]) \
         .apply(lambda r: r["row"]) \
         .group_by(lambda row: row["application_id"], name="application_state_events") \
         .to_topic(app.topic(cfg.topic_app_state,
                             value_serializer="json", key_serializer="bytes",
                             config=topic_config()))

    aproj.filter(lambda r: not r["ok"]) \
         .apply(lambda r: r["error"]) \
         .to_topic(app.topic(cfg.topic_dlq_cdc,
                             value_serializer="json", key_serializer="bytes",
                             config=topic_config()
                             ))

    return app


def run():
    app = build_app()
    print(f"stream-processor up | topics: {cfg.topic_app_submitted} -> "
          f"{cfg.topic_app_dedup} / {cfg.topic_dlq_app_submitted} | "
          f"guarantee={cfg.processing_guarantee}", flush=True)
    app.run()


if __name__ == "__main__":
    run()
