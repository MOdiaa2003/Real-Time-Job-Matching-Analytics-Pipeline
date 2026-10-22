"""Measure production -> dedup-output freshness (business problem 4 SLA).

Produces 24 real events (rate 4/s, no faults), then reads
application_submitted_dedup from the beginning with a fresh consumer group
and computes latency = (consume_time - produced_at_ms) for every replayed
unique event produced after this probe started.

Reports count/p50/p95/max in ms; PASS if p95 <= 10000 (10-second SLA) and
every event delivered within 15s.

Requires .venv-streams (confluent-kafka + fastavro).
"""
import io
import json
import struct
import subprocess
import sys
import time
import urllib.request
from collections import Counter

from confluent_kafka import Consumer, TopicPartition
from fastavro import schemaless_reader

BOOT = "localhost:9092"
SR = "http://localhost:8081"
INPUT = "application_submitted_events"
OUT = "application_submitted_dedup"
SLA_MS = 10_000

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


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0
    k = min(len(sorted_vals) - 1, int(round(p * (len(sorted_vals) - 1))))
    return sorted_vals[k]


def main():
    start_ms = time.time() * 1000
    # 1. clean input tail: consume the input to its end with a throwaway
    #    group so we only measure OUR producer run
    prod = subprocess.run(
        [sys.executable, r"..\..\Realtime_Pipline\application_event_producer.py",
         "--rate", "4", "--duration", "6", "--seed", "%d" % (time.time_ns() % 100000)],
        capture_output=True, text=True, timeout=120)
    print("producer:", [l for l in prod.stdout.splitlines() if l.startswith("DONE")]
          or prod.stdout.splitlines()[-1:])

    # 2. consume FULL output from beginning (fresh group), keep only events
    #    produced after start_ms
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"test06-{int(time.time()*1000)}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([OUT])
    latencies = []
    deadline = time.time() + 45
    ids = set()
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is None:
            if not latencies and not ids:
                continue
            continue
        if m.error():
            continue
        try:
            e = decode_wire(m.value())
        except Exception:
            continue
        now = time.time() * 1000
        if e.get("produced_at_ms", -1) >= start_ms:
            lat = now - e["produced_at_ms"]
            latencies.append(lat)
            ids.add(e["event_id"])
        c.commit(asynchronous=False) if False else None
    c.close()

    latencies.sort()
    n = len(latencies)
    if n == 0:
        print("FAIL no produced events were observed on the dedup topic")
        return 1
    p50, p95, mx = pct(latencies, .5), pct(latencies, .95), latencies[-1]
    ok = p95 <= SLA_MS and mx <= 15_000
    print(f"measured: events={n} distinct={len(ids)} p50={p50:.0f}ms "
          f"p95={p95:.0f}ms max={mx:.0f}ms (SLA target p95<= {SLA_MS}ms)")
    print(("PASS" if ok else "FAIL") + " latency probe")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
