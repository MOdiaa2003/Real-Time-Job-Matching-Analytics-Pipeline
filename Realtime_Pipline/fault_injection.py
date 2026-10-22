#!/usr/bin/env python3
"""
fault_injection.py - on-demand fault injection helpers
=====================================================
Each helper injects ONE specific failure/burden class into the
application_submitted_events stream so acceptance tests can be triggered
deterministically (this module is a library: the CLI lives inside
application_event_producer.py, which consumes these helpers):

  duplication   -> same event_id replayed N times      (tests/02)
  malformed     -> bytes that are not valid Avro per the registered
                   schema / Confluent wire format      (tests/05)
  late events   -> event_time_ms skewed into the past  (tests/04, grace period)
  burst         -> > ALERT_APPLY_LIMIT applies per candidate in a short
                   window (spam detector + metrics)    (tests/04)
"""

import argparse
import json
import os
import time
import uuid
import datetime


def make_event_id():
    return str(uuid.uuid4())


def make_event(candidate_id, job_id, event_id=None, when_ms=None,
               source="synthetic_test", status="submitted",
               application_id=None, trace_id=None):
    """One valid ApplicationSubmittedEvent as a Python dict (Avro-compatible)."""
    if event_id is None:
        event_id = make_event_id()
    if when_ms is None:
        when_ms = time.time() * 1000.0
    when_ms = int(when_ms)
    applied_at = datetime.datetime.utcfromtimestamp(when_ms / 1000.0).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    return {
        "event_id": event_id,
        "candidate_id": candidate_id,
        "job_id": job_id,
        "application_id": application_id,
        "status": status,
        "applied_at": applied_at,
        "event_time_ms": when_ms,
        "produced_at_ms": -1,          # producer stamps this on send
        "source": source,
        "trace_id": trace_id,
    }


def make_malformed_payloads(kind="non_avro_bytes", n=1):
    """Bytes that MUST fail downstream deserialization/validation.

    kinds:
      non_avro_bytes           : random junk bytes (violates the 5-byte
                                 Confluent wire header -> Avro decode fails)
      wrong_header             : correct magic byte + bogus schema id ->
                                 DLQ should carry the decode-error metadata
      valid_schema_wrong_types : raw JSON text whose field types violate
                                 the schema (int event_id, bool job_id...)
    """
    payloads = []
    for _ in range(n):
        if kind == "wrong_header":
            payloads.append(b"\x00" + uuid.uuid4().bytes[:4] + b"{'junk':1}")
        elif kind == "valid_schema_wrong_types":
            payloads.append(json.dumps({"event_id": 123, "candidate_id": None,
                                        "job_id": True,
                                        "event_time_ms": "not-a-long"}
                                       ).encode("utf-8"))
        else:  # non_avro_bytes (default)
            payloads.append(os.urandom(48))
    return payloads


def burst_events(candidates, jobs_per_candidate, when_ms=None, rate_multiplier=1):
    """candidate x jobs grid for the metrics/alerts acceptance tests.

    One candidate applies to `jobs_per_candidate` DISTINCT jobs inside a
    short wall-clock span -> 5-min window contains > ALERT_APPLY_LIMIT rows.
    """
    if when_ms is None:
        when_ms = time.time() * 1000.0
    events = []
    for cand in candidates:
        for j in range(jobs_per_candidate * rate_multiplier):
            events.append(make_event(cand, f"JOB_BURST_{cand[-4:]}_{j:03d}",
                                     when_ms=when_ms))
    return events


def cli_flags(parser):
    """Attach the standard fault-injection flags to an argparse parser."""
    g = parser.add_argument_group("fault injection")
    g.add_argument("--duplicate-rate", type=float, default=0.0,
                   help="fraction (0..1) of events replayed N times "
                        "(N = --duplicate-times)")
    g.add_argument("--duplicate-times", type=int, default=3,
                   help="replays per duplicated event")
    g.add_argument("--malformed-rate", type=float, default=0.0,
                   help="fraction (0..1) of events sent as invalid payloads")
    g.add_argument("--malformed-kind", default="non_avro_bytes",
                   choices=["non_avro_bytes", "wrong_header",
                            "valid_schema_wrong_types"])
    g.add_argument("--late-event-rate", type=float, default=0.0,
                   help="fraction (0..1) of events whose event_time_ms is "
                        "skewed back by --late-seconds")
    g.add_argument("--late-seconds", type=int, default=3600,
                   help="event-time skew in seconds for --late-event-rate")
    g.add_argument("--burst", action="store_true",
                   help="burst mode: --burst-candidates x --burst-jobs applies")
    g.add_argument("--burst-candidates", type=int, default=2)
    g.add_argument("--burst-jobs", type=int, default=8,
                   help="distinct jobs applied to per burst candidate")
    return parser


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Library of fault-injection helpers "
                    "(use application_event_producer.py to run them)")
    parser = cli_flags(parser)
    args = parser.parse_args()
    # When invoked directly, only the helpers are demonstrated (no broker):
    print("Pure helper module - no broker I/O. Demonstration only.")
    print("sample event:", json.dumps(make_event("cand_synth", "job_synth"),
                                     indent=2))
    print("malformed sample:",
          make_malformed_payloads(args.malformed_kind, 1)[0][:32], "...")
