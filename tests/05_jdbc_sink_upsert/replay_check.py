"""Idempotency check for the metrics JDBC sink:

1. read the LATEST record from job_metrics_5m (raw key/value bytes)
2. snapshot the analytics-DB row count for that (job_id, window pair) PK
3. RE-PRODUCE the same record to the topic (a replay, e.g. after a sink
   restart that resets offsets)
4. wait for the sink to process it
5. require: row COUNT for the PK unchanged AND the row's counter values
   identical to the replayed record -> upsert, not duplicate-append and
   not double-counting.
"""
import io
import json
import os
import struct
import subprocess
import sys
import time
import urllib.request

from confluent_kafka import Consumer, Producer, TopicPartition

BOOT = "localhost:9092"
SR = "http://localhost:8081"
TOPIC = "job_metrics_5m"
DSN = ["docker", "exec", "my_postgres_analytics", "psql", "-U", "postgres",
       "-d", "analytics", "-t", "-A", "-c"]


def get_schema(schema_id):
    with urllib.request.urlopen(f"{SR}/schemas/ids/{schema_id}") as r:
        return json.loads(json.loads(r.read())["schema"])


def decode_value(b):
    sid, = struct.unpack(">I", b[1:5])
    schema = get_schema(sid)
    return schemaless_reader(io.BytesIO(b[5:]), schema), sid


from fastavro import schemaless_reader  # noqa: E402


def db_scalar(sql):
    out = subprocess.run(DSN + [sql], capture_output=True, text=True, timeout=30)
    return out.stdout.strip()


def main():
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"test05-{int(time.time())}",
                  "enable.auto.commit": False})
    parts = len(c.list_topics(TOPIC).topics[TOPIC].partitions)
    hi, chosen = {}, {}
    for p in range(parts):
        lo, h = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
        hi[p] = h
        if h > 0:
            chosen = {"p": p, "lo": lo, "hi": h}
    if not chosen:
        print("FAIL topic job_metrics_5m empty")
        return 1
    p = chosen["p"]
    c.assign([TopicPartition(TOPIC, p, chosen["hi"] - 1)])
    msg = None
    deadline = time.time() + 30
    while msg is None and time.time() < deadline:
        m = c.poll(0.5)
        if m and not m.error():
            msg = m
    c.close()
    if msg is None:
        print("FAIL could not read latest record")
        return 1

    job_id, window_start, window_end = None, None, None
    try:
        data, sid = decode_value(msg.value())
        for k in ("JOB_ID_FLAT", "job_id", "JO_ID_FLAT"):
            if k in data:
                job_id = data[k]
        window_start = data.get("WINDOW_START", data.get("window_start"))
        window_end = data.get("WINDOW_END", data.get("window_end"))
        apps_cnt = data.get("APPLICATIONS_CNT", data.get("applications_cnt"))
        matches_cnt = data.get("MATCHES_CNT", data.get("matches_cnt"))
        if job_id is None or window_start is None:
            print(f"FAIL decoded value lacks key fields: {data}")
            return 1
        print(f"OK replaying record for {job_id} window[{window_start}..{window_end}] "
              f"apps={apps_cnt} matches={matches_cnt}")
    except Exception as e:
        print(f"FAIL could not decode latest record: {e}")
        return 1

    pk_count_before = int(db_scalar(
        f"SELECT COUNT(*) FROM job_metrics_5m WHERE job_id='{job_id}' "
        f"AND window_start={window_start} AND window_end={window_end};"))
    total_before = int(db_scalar("SELECT COUNT(*) FROM job_metrics_5m;"))

    # re-produce the EXACT record
    prod = Producer({"bootstrap.servers": BOOT, "acks": "all"})
    prod.produce(TOPIC, key=msg.key(), value=msg.value())
    prod.flush(15)
    print("OK record re-produced")

    # give the sink time to poll+upsert
    time.sleep(40)
    pk_count_after = int(db_scalar(
        f"SELECT COUNT(*) FROM job_metrics_5m WHERE job_id='{job_id}' "
        f"AND window_start={window_start} AND window_end={window_end};"))
    total_after = int(db_scalar("SELECT COUNT(*) FROM job_metrics_5m;"))
    row = db_scalar(
        f"SELECT applications_cnt || '/' || matches_cnt FROM job_metrics_5m "
        f"WHERE job_id='{job_id}' AND window_start={window_start} "
        f"AND window_end={window_end};")

    ok = (pk_count_after == 1 and total_after == total_before
          and row == f"{apps_cnt}/{matches_cnt}")
    print(("OK " if ok else "FAIL ")
          + f"PK rows before/after: {pk_count_before}/{pk_count_after}; "
            f"total: {total_before}/{total_after}; row now: {row}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
