"""
config.py - environment-driven configuration for the stream processor.
Kept in sync with .env.example / docker-compose.yml environment keys.
"""
import os


def _int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


class Config:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

    consumer_group = os.getenv("SP_CONSUMER_GROUP", "app-submitted-dedup")
    state_dir = os.getenv("SP_STATE_DIR", "/var/lib/quix-state")

    topic_app_submitted = os.getenv("TOPIC_APP_SUBMITTED", "application_submitted_events")
    topic_app_dedup = os.getenv("TOPIC_APP_DEDUP", "application_submitted_dedup")
    topic_dlq_app_submitted = os.getenv("TOPIC_DLQ_APP_SUBMITTED", "dlq.application_submitted_events")
    topic_job_state = os.getenv("TOPIC_JOB_STATE", "job_state_events")
    topic_app_state = os.getenv("TOPIC_APP_STATE", "application_state_events")
    topic_dlq_cdc = os.getenv("TOPIC_DLQ_CDC", "dlq.cdc_projection")

    # CDC source topics (Debezium JSON, used for the flat projections)
    topic_job_events = os.getenv("TOPIC_JOB_EVENTS", "job_events")
    topic_application_events = os.getenv("TOPIC_APPLICATION_EVENTS", "application_events")

    # Quix Streams processing guarantee:
    #   "exactly-once"  -> transactional offsets+output (kafka EOS-v2 semantics)
    #   "at-least-once" -> lighter; business correctness for dedup is preserved
    #                      because the stateful dedup filters replays anyway.
    processing_guarantee = os.getenv("SP_PROCESSING_GUARANTEE", "exactly-once")

    # dedup state retention: replayed event_ids older than this are treated
    # as "new again" (bounded state memory; mirrors brief's 30m default)
    retention_ms = _int("DUPLICATION_STATE_RETENTION_MS", 30 * 60 * 1000)

    topic_partitions = _int("TOPIC_PARTITIONS", 3)
    replication_factor = 1

    # decode Cache size (schema id -> parsed schema)
    schema_cache_size = _int("SP_SCHEMA_CACHE_SIZE", 128)


cfg = Config()
