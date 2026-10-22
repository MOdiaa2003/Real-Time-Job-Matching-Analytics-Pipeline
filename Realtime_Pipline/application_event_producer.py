#!/usr/bin/env python3
"""
application_event_producer.py
=============================
Simulates the WEB APP's application submissions: the parent pipeline data
still flows ONLY through PostgreSQL->Debezium (CDC); this producer adds a
second, INDEPENDENT ingestion path - `application_submitted_events` -
exactly like a real product would have its own click/submission stream.

Design points mapped to business requirements:
  * IDEMPOTENT producer  : enable.idempotence=true + acks=all -> broker-side
    dedup on retries; key=job_id keeps per-job ordering.
  * Avro + Schema Registry: writer schema from schemas/application_submitted_event.avsc,
    subject application_submitted_events-value, BACKWARD compatibility.
  * Fault injection flags (--duplicate-rate/--malformed-rate/--late-event-rate/--burst)
    let every acceptance test be triggered on demand.

Synthetic data only. Run:
  python application_event_producer.py --rate 10 --duration 30
  python application_event_producer.py --duplicate-rate 1.0 --duration 10
  python application_event_producer.py --malformed-rate 0.2 --duration 10
  python application_event_producer.py --late-event-rate 0.5 --duration 10
  python application_event_producer.py --burst --duration 5
"""

import argparse
import os
import random
import sys
import time
import datetime

try:
    from confluent_kafka import Producer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
except ImportError as e:
    print("Missing dependency (pip install confluent-kafka[avro]):", e)
    raise SystemExit(1)

# import fault_injection from the same folder
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fault_injection import (make_event, make_malformed_payloads,  # noqa: E402
                             burst_events, cli_flags)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA_PATH = os.path.join(ROOT_DIR, "schemas", "application_submitted_event.avsc")


def load_schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return f.read()


def delivery_report(err, msg):
    global SENT_OK, SENT_FAIL
    if err is not None:
        SENT_FAIL += 1
        # idempotent producers retry safely: a failure here means the
        # broker refused with delivery timeout, not a duplicate risk.
        sys.stderr.write(f"[delivery-fail] {err}\n")
    else:
        SENT_OK += 1
SENT_OK = 0
SENT_FAIL = 0


def main():
    ap = argparse.ArgumentParser(
        description="Avro+SR idempotent producer for application_submitted_events")
    ap = cli_flags(ap)
    ap.add_argument("--rate", type=float, default=10.0, help="events/sec")
    ap.add_argument("--duration", type=int, default=30, help="seconds (0=until Ctrl-C)")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    ap.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    ap.add_argument("--topic", default="application_submitted_events")
    ap.add_argument("--candidate-pool", type=int, default=50)
    ap.add_argument("--job-pool", type=int, default=200)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    schema_str = load_schema()
    sr_client = SchemaRegistryClient({"url": args.schema_registry})
    serializer = AvroSerializer(sr_client, schema_str)
    ctx = SerializationContext(args.topic, MessageField.VALUE)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,   # exactly one copy per (pid,seq) even on retries
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,
        # NOTE: 'enable.thread.pin' existed in older librdkafka builds;
        # confluent-kafka 2.9 rejects it as an unknown property - dropped.
        "message.timeout.ms": 60000,
    })

    def send(job_id, value_bytes):
        producer.produce(args.topic, key=job_id.encode("utf-8"),
                         value=value_bytes,
                         on_delivery=delivery_report)

    def random_ids():
        cand = f"CAND_{random.randint(0, args.candidate_pool):06d}"
        job = f"JOB_{random.randint(0, args.job_pool):06d}"
        return cand, job

    # Burst pre-generation (metrics/alerts tests need DETERMINISTIC ids)
    pending_burst = []
    if args.burst:
        candidates = [f"BURSTCAND_{i:03d}" for i in range(args.burst_candidates)]
        pending_burst = burst_events(candidates, args.burst_jobs)
        print(f"burst: {len(candidates)} candidates x {args.burst_jobs} jobs "
              f"= {len(pending_burst)} events ready")

    stats = {"sent": 0, "duplicated": 0, "malformed": 0, "late": 0, "burst": 0}
    start = time.time()
    interval = 1.0 / max(args.rate, 0.01)
    next_tick = start
    burst_idx = 0

    while True:
        elapsed = time.time() - start
        if args.duration and elapsed >= args.duration:
            break
        if not args.burst and pending_burst:
            pass

        # ── choose what to emit this tick ─────────────────────────────
        event = None
        malformed = None

        r = random.random()
        if r < args.malformed_rate:
            payloads = make_malformed_payloads(args.malformed_kind, 1)
            cand, job = random_ids()
            send(job, payloads[0])
            stats["malformed"] += 1
        elif args.burst and burst_idx < len(pending_burst):
            event = pending_burst[burst_idx]
            burst_idx += 1
            stats["burst"] += 1
        elif not args.burst:
            cand, job = random_ids()
            late = random.random() < args.late_event_rate
            when_ms = time.time() * 1000.0
            if late:
                when_ms -= args.late_seconds * 1000.0
                stats["late"] += 1
            event = make_event(cand, job, when_ms=when_ms)
        else:
            time.sleep(0.05)
            continue

        if event is not None:
            job = event["job_id"]
            event["produced_at_ms"] = int(time.time() * 1000)
            payload = serializer(event, ctx)
            send(job, payload)
            stats["sent"] += 1

            # ── duplicate injection: SAME event_id replayed N times ──────
            if random.random() < args.duplicate_rate:
                for _ in range(max(1, args.duplicate_times - 1)):
                    send(job, payload)
                    stats["duplicated"] += 1

        # tick pacing
        next_tick += interval
        sleep_for = next_tick - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_tick = time.time()  # we are behind; drop the backlog

        if int(elapsed) != int(elapsed - interval) and int(elapsed) % 10 == 0:
            print(f"[{int(elapsed):03d}s] stats={stats} sent_ok={SENT_OK} "
                  f"sent_fail={SENT_FAIL}", flush=True)

    producer.flush(30)
    stats["sent"] = SENT_OK
    stats["failed"] = SENT_FAIL
    print("DONE:", stats)


if __name__ == "__main__":
    main()
