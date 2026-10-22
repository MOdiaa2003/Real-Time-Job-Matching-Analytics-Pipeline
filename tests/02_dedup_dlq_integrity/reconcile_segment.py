"""Reconcile exactly one SEGMENT of the dedup round trip.

Two-phase usage (called by tests/02_dedup_dlq_integrity/run.ps1):
  python reconcile_segment.py record   -> writes segment_before.json
  python reconcile_segment.py check    -> reads segment_before.json, consumes
                                          the [before, now) segment from
  - application_submitted_events   (Confluent Avro wire, some records invalid)
  - application_submitted_dedup    (Confluent Avro wire, unique events)
  - dlq.application_submitted_events (JSON, DUPLICATE / *_INVALID records)
and verifies 100% accounting:
  for every event_id: input_count == output_count + dlq_count
  every input record that is not decodable Avro must appear as one DLQ record.

Requires .venv-streams (confluent-kafka + fastavro).
"""
import io
import json
import struct
import sys
import time
import urllib.request
from collections import Counter, defaultdict

from confluent_kafka import Consumer, TopicPartition
from fastavro import schemaless_reader

BOOT = "localhost:9092"
SR = "http://localhost:8081"
TOPICS = {
    "input": "application_submitted_events",
    "output": "application_submitted_dedup",
    "dlq": "dlq.application_submitted_events",
}
STATE = "segment_before.json"
WAIT_SECS = 40

_schema_cache = {}


def get_schema(schema_id):
    if schema_id not in _schema_cache:
        with urllib.request.urlopen(f"{SR}/schemas/ids/{schema_id}") as r:
            _schema_cache[schema_id] = json.loads(json.loads(r.read())["schema"])
    return _schema_cache[schema_id]


def decode_wire(b):
    if len(b) < 5 or b[0] != 0:
        raise ValueError("bad wire header")
    sid, = struct.unpack(">I", b[1:5])
    return schemaless_reader(io.BytesIO(b[5:]), get_schema(sid))


def end_offsets(admin, topic):
    from confluent_kafka import TopicPartition as TP
    out = []
    meta = admin.list_topics(topic).topics[topic]
    for p in range(len(meta.partitions)):
        tp = TP(topic, p)
        lo, hi = admin.get_watermark_offsets(tp, timeout=10)
        out.append((p, hi))
    return out


def main():
    mode = sys.argv[1]
    group = f"test02-{mode}-{int(time.time())}"
    admin_kafka = Consumer({"bootstrap.servers": BOOT, "group.id": group,
                            "enable.auto.commit": False})

    ends = {}
    for role, topic in TOPICS.items():
        ends[role] = dict(end_offsets(admin_kafka, topic))

    if mode == "record":
        with open(STATE, "w") as f:
            json.dump(ends, f)
        print("recorded", ends)
        admin_kafka.close()
        return

    with open(STATE) as f:
        before = json.load(f)

    records = defaultdict(list)  # role -> list of raw bytes
    tp_list = []
    starts = {}
    for role, topic in TOPICS.items():
        for p, end in ends[role].items():
            start = before[role].get(str(p), 0)
            if end > start:
                tp_list.append(TopicPartition(topic, p, start))
                starts[(role, p)] = start

    admin_kafka.assign(tp_list)
    wanted = sum(e - starts[(r, p)] for r, topic in TOPICS.items()
                 for p, e in ends[r].items())
    got = 0
    deadline = time.time() + WAIT_SECS
    while got < wanted and time.time() < deadline:
        m = admin_kafka.poll(0.5)
        if m is None or m.error():
            continue
        role = {v: k for k, v in TOPICS.items()}[m.topic()]
        if m.value() is not None:
            records[role].append(m.value())
            got += 1
    admin_kafka.close()

    # ---- analyze ---------------------------------------------------------
    input_ids = Counter()
    bad_wire = 0
    for b in records["input"]:
        try:
            e = decode_wire(b)
            input_ids[e["event_id"]] += 1
        except Exception:
            bad_wire += 1

    out_ids = Counter()
    out_bad = 0
    for b in records["output"]:
        try:
            e = decode_wire(b)
            out_ids[e["event_id"]] += 1
        except Exception:
            out_bad += 1

    dlq_wires = Counter()
    dlq_with_ids = defaultdict(Counter)  # error_code -> event_id counts
    for b in records["dlq"]:
        try:
            p = json.loads(b)
            code = p.get("error_code", "UNSPECIFIED")
            dlq_wires[code] += 1
            ev = p.get("event") or {}
            if ev.get("event_id"):
                dlq_with_ids[code][ev["event_id"]] += 1
        except Exception:
            dlq_wires["UNPARSEABLE"] += 1

    # ---- checks ----------------------------------------------------------
    lines, ok = [], True

    def expect(cond, msg):
        nonlocal ok
        if not cond:
            ok = False
        lines.append(("OK " if cond else "FAIL ") + msg)

    dup_extra = sum(v - 1 for v in input_ids.values() if v > 1)
    expect(out_bad == 0, f"dedup output decodable records: output_unparsable={out_bad}")
    expect(dlq_wires.get("UNPARSEABLE", 0) == 0,
           f"DLQ records are parseable JSON (dlq_codes={dict(dlq_wires)})")
    expect(bad_wire == dlq_wires.get("BAD_WIRE_HEADER", 0) + dlq_wires.get("INVALID", 0),
           f"malformed input -> DLQ: input_bad_wire={bad_wire} dlq={dict(dlq_wires)}")
    expect(dup_extra == dlq_wires.get("DUPLICATE", 0),
           f"replayed copies -> DLQ: input_replays={dup_extra} dlq_duplicate={dlq_wires.get('DUPLICATE', 0)}")

    missing, mismatches = [], []
    all_ids = set(input_ids) | set(out_ids)
    for eid in all_ids:
        replays_expected = input_ids[eid] - 1  # the base copy goes to output
        got_out = out_ids[eid]
        got_dlq = (dlq_with_ids.get("DUPLICATE", {}).get(eid, 0)
                   + dlq_with_ids.get("INVALID", {}).get(eid, 0))
        if got_out == 0 and got_dlq != replays_expected:
            # Base copy never emitted AND DLQ count does not match the replay
            # count inside this segment -> the base was silently dropped.
            missing.append(eid)
        elif got_out > 0 and (got_out - 1) + got_dlq != replays_expected:
            mismatches.append((eid, replays_expected, got_out, got_dlq))
    expect(not missing, f"no event lost without output/DLQ trace (missing={missing[:3]})")
    expect(not mismatches, f"per-id accounting holds for all ids "
           f"(first_mismatch={mismatches[:2]})")

    print("PASS" if ok else "FAIL")
    for l in lines:
        print("  " + l)
    print(f"summary: input_decodable={len(input_ids)} input_replays={dup_extra} "
          f"input_bad_wire={bad_wire} output_rows={sum(out_ids.values())} "
          f"dlq={dict(dlq_wires)}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
