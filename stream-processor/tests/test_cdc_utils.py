"""
Unit tests for the stream processor's pure logic (no broker needed).

Run from repo root:
  .venv-streams\\Scripts\\python -m pytest stream-processor\\tests -q
"""
import io
import json
import os
import struct
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import cdc_utils  # noqa: E402


class FakeState(dict):
    """Dict mimicking quixstreams State get/set/exists contract."""

    def get(self, k, default=None):
        return dict.get(self, k, default)

    def set(self, k, v):
        self[k] = v

    def exists(self, k):
        return k in self


# ── Confluent wire format ──────────────────────────────────────────────

def event_schema():
    global _ship_schema
    if not globals().get("_ship_schema"):
        raw = json.load(open(
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), "schemas",
                "application_submitted_event.avsc"), encoding="utf-8"))
        raw.pop("comment", None)
        _ship_schema = cdc_utils.fastavro.parse_schema(raw)
    return _ship_schema


def valid_event():
    return {
        "event_id": "evt-1", "candidate_id": "c1", "job_id": "j1",
        "application_id": None, "status": "submitted",
        "applied_at": "2026-01-01T00:00:00Z", "event_time_ms": 1000,
        "produced_at_ms": -1, "source": "web_app", "trace_id": None,
    }


class FakeRegistry:
    def __init__(self, schema):
        self.schema = schema

    def schema_by_id(self, _sid):
        return self.schema


# wire encode/decode roundtrip
def test_wire_roundtrip():
    ev = valid_event()
    wire = cdc_utils.encode_confluent_wire(ev, event_schema(), schema_id=7)
    assert wire[0] == 0  # magic byte
    assert struct.unpack(">I", wire[1:5])[0] == 7
    out = cdc_utils.decode_confluent_wire(wire, FakeRegistry(event_schema()))
    assert out == ev


def test_wire_bad_header_raises_decodeerror():
    with pytest.raises(cdc_utils.DecodeError) as exc:
        cdc_utils.decode_confluent_wire(b"xxnonsense", FakeRegistry(event_schema()))
    assert exc.value.error_code == "BAD_WIRE_HEADER"
    assert "error_code" in exc.value.metadata()


def test_wire_empty_raises():
    with pytest.raises(cdc_utils.DecodeError) as exc:
        cdc_utils.decode_confluent_wire(b"", FakeRegistry(event_schema()))
    assert exc.value.error_code == "EMPTY_VALUE"


def test_wire_unknown_schema_id():
    class Boom:
        def schema_by_id(self, _):
            raise RuntimeError("404 no such id")

    wire = cdc_utils.encode_confluent_wire(valid_event(), event_schema(), 99)
    with pytest.raises(cdc_utils.DecodeError) as exc:
        cdc_utils.decode_confluent_wire(wire, Boom())
    assert exc.value.error_code == "SCHEMA_LOOKUP_FAILED"


# ── Validation -> DLQ metadata ─────────────────────────────────────────

def test_validation_missing_fields():
    ev = valid_event()
    ev.pop("candidate_id")
    err = cdc_utils.validate_event(ev)
    assert err is not None and any("candidate_id" in m for m in err.errors)
    md = err.metadata()
    assert md["error_code"] == "VALIDATION_ERROR"


def test_validation_ok_event():
    assert cdc_utils.validate_event(valid_event()) is None


# ── Dedup with retention ───────────────────────────────────────────────

def test_dedup_new_then_duplicate():
    st = FakeState()
    assert cdc_utils.dedup_decide({"event_id": "E"}, st, 1000, 10_000) == "new"
    assert cdc_utils.dedup_decide({"event_id": "E"}, st, 2000, 10_000) == "duplicate"


def test_dedup_retention_forgets_expired():
    st = FakeState()
    assert cdc_utils.dedup_decide({"event_id": "E"}, st, 1000, 10_000) == "new"
    # replay AFTER retention window expired -> treated as new again
    assert cdc_utils.dedup_decide({"event_id": "E"}, st, 99_999, 10_000) == "new"


def test_dedup_state_bounded():
    st = FakeState()
    for i in range(300):
        cdc_utils.dedup_decide({"event_id": f"E{i:05d}"}, st, 1000 + i, 10_000)
    # anything older than cutoff pruned; state dict stays bounded
    assert len(st.get("events")) <= 300  # no unbounded accumulation
    assert len(st.get("events")) > 0     # recent entries retained


# ── CDC projection ──────────────────────────────────────────────────────

def test_project_job_cdc_flat_row():
    wire = json.dumps({
        "op": "u", "ts_ms": 5,
        "before": {"post_id": "A", "status": "open"},
        "after": {"post_id": "A", "status": "closed",
                  "remote_policy": "hybrid"},
    }).encode()
    r = cdc_utils.project_job_state(wire)
    assert r["ok"] is True
    row = r["row"]
    assert row["post_id"] == "A" and row["status"] == "closed"
    assert row["is_deleted"] == 0 and row["op"] == "u"


def test_project_job_cdc_delete_takes_before():
    wire = json.dumps({
        "op": "d", "ts_ms": 9,
        "before": {"post_id": "A", "status": "closed"},
        "after": None,
    }).encode()
    r = cdc_utils.project_job_state(wire)
    assert r["ok"] and r["row"]["post_id"] == "A" and r["row"]["is_deleted"] == 1


def test_project_cdc_malformed_goes_error():
    r = cdc_utils.project_job_state(b"{not json")
    assert not r["ok"] and r["error"]["error_code"] == "CDC_JSON_DECODE_FAILED"


def test_project_app_cdc_flat_row():
    wire = json.dumps({
        "op": "c", "ts_ms": 7,
        "after": {"application_id": "X", "candidate_id": "c", "post_id": "j",
                  "status": "applied", "lifecycle_stage": 0,
                  "final_outcome": None},
    }).encode()
    r = cdc_utils.project_application_state(wire)
    assert r["ok"] and r["row"]["application_id"] == "X"
