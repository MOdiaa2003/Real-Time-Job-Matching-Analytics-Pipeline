"""
cdc_utils.py - pure helpers for the stream processor
====================================================
NO Kafka/quix-types here on purpose: every function below is unit-testable
with plain inputs (see tests/test_cdc_utils.py).

Covers:
  1. Confluent Avro wire-format encode/decode (5-byte header: magic 0x00 +
     big-endian uint32 schema id) - needed because ksqlDB expects Avro on
     application_submitted_dedup while we decode raw bytes ourselves from
     application_submitted_events.
  2. Event validation -> DLQ routing metadata (business problem #7).
  3. Stateful dedup decision with retention (business problem #3).
  4. Flat CDC projection for ksqlDB (Debezium JSON envelope -> simple row),
     so ksql never has to parse nested Debezium structs at all.
"""
import json
import struct
import io

import fastavro
import requests


# ── Errors ────────────────────────────────────────────────────────────

class DecodeError(Exception):
    """Value did not deserialize to a valid event."""

    def __init__(self, message, error_code="DECODE_ERROR", raw_len=0, stage="decode"):
        super().__init__(message)
        self.error_code = error_code
        self.raw_len = raw_len
        self.stage = stage

    def metadata(self):
        return {
            "error_code": self.error_code,
            "error_message": str(self)[:300],
            "stage": self.stage,
            "raw_value_len": self.raw_len,
        }


class ValidationError(Exception):
    """Event decoded but failed business validation."""

    def __init__(self, errors):
        super().__init__("; ".join(errors))
        self.errors = errors

    def metadata(self):
        return {
            "error_code": "VALIDATION_ERROR",
            "error_message": "; ".join(self.errors)[:300],
            "stage": "validate",
        }


# ── Schema Registry (minimal client; schemas are cached) ─────────────

class RegistrySchemaCache:
    """Fetches + parses Avro writer schemas by id from Confluent Schema Registry."""

    def __init__(self, url, cache_size=128):
        self.url = url.rstrip("/")
        self._cache = {}
        self._order = []
        self.cache_size = cache_size

    def schema_by_id(self, schema_id):
        if schema_id in self._cache:
            return self._cache[schema_id]
        resp = requests.get(f"{self.url}/schemas/ids/{schema_id}", timeout=10)
        resp.raise_for_status()
        raw = json.loads(resp.text)
        parsed = fastavro.parse_schema(json.loads(raw["schema"]))
        self._cache[schema_id] = parsed
        self._order.append(schema_id)
        if len(self._order) > self.cache_size:
            evict = self._order.pop(0)
            self._cache.pop(evict, None)
        return parsed

    def schema_by_subject_version(self, subject, version="latest"):
        resp = requests.get(f"{self.url}/subjects/{subject}/versions/{version}",
                            timeout=10)
        resp.raise_for_status()
        raw = json.loads(resp.text)
        return raw["id"], fastavro.parse_schema(json.loads(raw["schema"]))

    def register(self, subject, schema_dict):
        """Convenience for tests: POST schema and return the registry id."""
        payload = {"schemaType": "AVRO",
                   "schema": json.dumps(schema_dict)}
        resp = requests.post(f"{self.url}/subjects/{subject}/versions",
                             json=payload, timeout=10)
        if resp.status_code == 200 or resp.status_code == 201:
            return resp.json()["id"]
        if resp.status_code == 409:
            raise ValueError(f"schema incompatible for subject {subject}: {resp.text}")
        raise ValueError(f"schema registration failed: {resp.status_code} {resp.text}")


# ── Wire format ───────────────────────────────────────────────────────

WIRE_MAGIC = 0x00  # Confluent Avro magic byte


def decode_confluent_wire(value_bytes, registry_cache, reader_schema=None):
    """Decode Confluent wire format ((byte)0x00 + 4b id + Avro payload).

    Raises DecodeError with error metadata the DLQ record carries.
    """
    if not value_bytes:
        raise DecodeError("empty value", error_code="EMPTY_VALUE",
                          raw_len=len(value_bytes or b""), stage="decode")
    buf = io.BytesIO(value_bytes)
    magic = buf.read(1)
    if magic != bytes([WIRE_MAGIC]):
        raise DecodeError(
            f"bad magic byte: {magic.hex() if magic else '<none>'} (expected 0x00); "
            "payload is not Confluent-Avro encoded",
            error_code="BAD_WIRE_HEADER", raw_len=len(value_bytes), stage="decode")
    sid_b = buf.read(4)
    if len(sid_b) != 4:
        raise DecodeError("truncated schema id", error_code="TRUNCATED_HEADER",
                          raw_len=len(value_bytes), stage="decode")
    schema_id = struct.unpack(">I", sid_b)[0]
    try:
        writer_schema = registry_cache.schema_by_id(schema_id)
    except Exception as exc:  # noqa: BLE001 - DLQ needs the failure detail
        raise DecodeError(f"schema id {schema_id} lookup failed: {exc}",
                          error_code="SCHEMA_LOOKUP_FAILED",
                          raw_len=len(value_bytes), stage="decode") from exc
    try:
        event = fastavro.schemaless_reader(buf, writer_schema, reader_schema)
    except Exception as exc:  # noqa: BLE001
        raise DecodeError(f"avro decode failed for schema id {schema_id}: {exc}",
                          error_code="AVRO_DECODE_FAILED",
                          raw_len=len(value_bytes), stage="decode") from exc
    return event


def encode_confluent_wire(event, schema, schema_id):
    buf = io.BytesIO()
    buf.write(bytes([WIRE_MAGIC]))
    buf.write(struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, schema, event)
    return buf.getvalue()


# ── Validation ────────────────────────────────────────────────────────

def validate_event(event):
    """Return a ValidationError if the decoded event breaks business rules."""
    errors = []
    if not isinstance(event, dict):
        return ValidationError(["decoded value is not an object"])
    for field in ("event_id", "candidate_id", "job_id"):
        v = event.get(field)
        if v is None or v == "":
            errors.append(f"missing required field: {field}")
        elif not isinstance(v, str):
            errors.append(f"field {field} must be string, got {type(v).__name__}")
    if "event_time_ms" not in event:
        errors.append("missing event_time_ms")
    else:
        t = event["event_time_ms"]
        if not isinstance(t, int) or isinstance(t, bool):
            errors.append("event_time_ms must be long")
        elif t <= 0:
            errors.append(f"event_time_ms non-positive: {t}")
    if errors:
        return ValidationError(errors)
    return None


# ── Dedup (page {State}-shaped mapping: get/exists/set/delete) ────────

def dedup_decide(event, state, now_ms, retention_ms):
    """Return 'new'|'duplicate' and update per-key state with retention.

    `state` is Quix `State` in production; anything dict-like with
    get/exists/set/delete works for tests.
    """
    event_id = event.get("event_id")
    if not event_id:
        return "duplicate"  # invalid already; routed as invalid by validation
    # per-key map: event_id -> first_seen_ms
    seen = state.get("events") or {}
    # retention prune FIRST (bounded per-key state): expired event_ids may
    # be seen "new" again, exactly like a Kafka Streams windowed dedup store
    cutoff = now_ms - retention_ms
    if seen:
        seen = {eid: ts for eid, ts in seen.items() if ts > cutoff}
        state.set("events", seen)
    if event_id in seen:
        return "duplicate"
    seen[event_id] = now_ms
    state.set("events", seen)
    return "new"


# ── CDC projection (Debezium JSON -> flat row for ksqlDB) ─────────────

def project_job_state(value_bytes):
    """Debezium job_events JSON -> flat row for the ksqlDB job-state table.

    Returns dict with ok/error flags; never raises (DLQ routing in app.py).
    """
    try:
        env = json.loads(value_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": {"error_code": "CDC_JSON_DECODE_FAILED",
                                       "error_message": str(exc)[:300],
                                       "stage": "cdc_decode",
                                       "raw_value_len": len(value_bytes or b"")}}
    op = env.get("op")
    before = env.get("before")
    after = env.get("after")
    row_image = after if after is not None else before
    if row_image is None or row_image.get("post_id") is None:
        return {"ok": False, "error": {"error_code": "CDC_MISSING_ID",
                                       "error_message": "no post_id in before/after",
                                       "stage": "cdc_decode",
                                       "raw_value_len": len(value_bytes or b"")}}
    status = (after or {}).get("status") or (before or {}).get("status")
    return {
        "ok": True,
        "row": {
            "post_id": row_image.get("post_id"),
            "status": status,
            "job_status": status,
            "op": op,
            "ts_ms": env.get("ts_ms") or 0,
            "remote_policy": (after or {}).get("remote_policy"),
            "is_deleted": 1 if (after is None and before is not None) else 0,
        },
    }


def project_application_state(value_bytes):
    """Debezium application_events JSON -> flat row for ksqlDB."""
    try:
        env = json.loads(value_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": {"error_code": "CDC_JSON_DECODE_FAILED",
                                       "error_message": str(exc)[:300],
                                       "stage": "cdc_decode",
                                       "raw_value_len": len(value_bytes or b"")}}
    op = env.get("op")
    before = env.get("before")
    after = env.get("after")
    row_image = after if after is not None else before
    if row_image is None or row_image.get("application_id") is None:
        return {"ok": False, "error": {"error_code": "CDC_MISSING_ID",
                                       "error_message": "no application_id in before/after",
                                       "stage": "cdc_decode",
                                       "raw_value_len": len(value_bytes or b"")}}
    return {
        "ok": True,
        "row": {
            "application_id": row_image.get("application_id"),
            "candidate_id": row_image.get("candidate_id"),
            "post_id": row_image.get("post_id"),
            "status": row_image.get("status"),
            "lifecycle_stage": row_image.get("lifecycle_stage"),
            "final_outcome": row_image.get("final_outcome"),
            "op": op,
            "ts_ms": env.get("ts_ms") or 0,
            "is_deleted": 1 if (after is None and before is not None) else 0,
        },
    }
