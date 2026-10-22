"""Reconcile the round-trip: input events vs dedup output vs DLQ."""
import collections
import json
import struct
import sys

from confluent_kafka import Consumer, TopicPartition
from fastavro import schemaless_reader
import io

BOOT = "localhost:9092"  # EXTERNAL listener (29092 is container-internal only)
SR = "http://localhost:8081"

import urllib.request

_schema_cache = {}

def get_schema(schema_id):
    if schema_id not in _schema_cache:
        with urllib.request.urlopen(f"{SR}/schemas/ids/{schema_id}") as r:
            _schema_cache[schema_id] = json.loads(json.loads(r.read())["schema"])
    return _schema_cache[schema_id]

def decode_wire(b):
    if len(b) < 5 or b[0] != 0:
        raise ValueError("bad wire")
    sid, = struct.unpack(">I", b[1:5])
    return schemaless_reader(io.BytesIO(b[5:]), get_schema(sid))

def consume(topic, limit=None):
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"audit-{sys.argv[1]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    recs = []
    import time
    deadline = time.time() + 15
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            continue
        recs.append(m)
        if limit and len(recs) >= limit:
            break
    c.close()
    return recs

inp = consume("application_submitted_events")
out = consume("application_submitted_dedup")
dlq = consume("dlq.application_submitted_events")

in_ids = collections.Counter()
bad_wire = 0
for m in inp:
    try:
        e = decode_wire(m.value())
        in_ids[e["event_id"]] += 1
    except Exception:
        bad_wire += 1

out_ids = collections.Counter()
for m in out:
    e = decode_wire(m.value())
    out_ids[e["event_id"]] += 1

dlq_codes = collections.Counter()
dlq_ids = collections.defaultdict(collections.Counter)
for m in dlq:
    p = json.loads(m.value())
    dlq_codes[p["error_code"]] += 1
    ev = p.get("event") or {}
    if ev.get("event_id"):
        dlq_ids[p["error_code"]][ev["event_id"]] += 1

dup_extra = sum(v - 1 for v in in_ids.values() if v > 1)
print(f"input records={len(inp)} wire_fail={bad_wire} distinct_event_ids={len(in_ids)} "
      f"replayed_copies={dup_extra}")
print(f"dedup_out records={len(out)} distinct={len(out_ids)}")
print(f"dlq codes={dict(dlq_codes)}")
print(f"input total including replays = {len(in_ids) + dup_extra} (+wire_fail {bad_wire})")

missing = [eid for eid in in_ids if eid not in out_ids]
print(f"distinct ids NOT on dedup output: {len(missing)}")
dup_dlq = sum(len(s) for s in dlq_ids.values())
print(f"DLQ with identifiable event ids: {dup_dlq}")
