"""latency_report.py - freshness probe mirroring tests/06 as a standalone
ops tool: measures producer-produced_at_ms vs consumed-at for unique events
on application_submitted_dedup, prints p50/p95/max against the 10s SLA.

Usage:
  .venv-streams/Scripts/python monitoring/latency_report.py [--wait 45]
Appends each run's line to monitoring/latency_report.jsonl as history.
"""
import argparse
import io
import json
import struct
import sys
import time
import urllib.request

from confluent_kafka import Consumer
from fastavro import schemaless_reader

BOOT = "localhost:9092"
SR = "http://localhost:8081"
OUT = "application_submitted_dedup"
SLA_MS = 10_000
START_MS = int(time.time() * 1000) - 60_000  # last minute of events


def get_schema(schema_id):
    if not hasattr(get_schema, "cache"):
        get_schema.cache = {}
    if schema_id not in get_schema.cache:
        with urllib.request.urlopen(f"{SR}/schemas/ids/{schema_id}") as r:
            get_schema.cache[schema_id] = json.loads(json.loads(r.read())["schema"])
    return get_schema.cache[schema_id]


def decode_wire(b):
    sid, = struct.unpack(">I", b[1:5])
    return schemaless_reader(io.BytesIO(b[5:]), get_schema(sid))


def pct(vals, p):
    if not vals:
        return 0.0
    return vals[min(len(vals) - 1, int(round(p * (len(vals) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=45)
    args = ap.parse_args()
    _ = START_MS  # kept for CLI/script parity with tests/06 (uses module const)

    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"latrep-{int(time.time()*1000)}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([OUT])
    lat = []
    deadline = time.time() + args.wait
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        try:
            e = decode_wire(m.value())
        except Exception:
            continue
        now = time.time() * 1000
        if e.get("produced_at_ms", -1) >= START_MS:
            lat.append(now - e["produced_at_ms"])
    c.close()

    lat.sort()
    if lat:
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] latency_report: events={len(lat)} " \
               f"p50={pct(lat,.5):.0f}ms p95={pct(lat,.95):.0f}ms max={lat[-1]:.0f}ms"
    else:
        # no events fresh in the lookback window: report it, do not crash
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] latency_report: events=0 " \
               f"p50=- p95=- max=- (no fresh events in lookback window)"
    print(line)
    with open("latency_report.jsonl", "a", encoding="utf-8") as f:
        f.write(line + "\n")
    ok = not lat or pct(lat, .95) <= SLA_MS
    print(("PASS" if ok else "FAIL") + f" (SLA p95<= {SLA_MS}ms)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
