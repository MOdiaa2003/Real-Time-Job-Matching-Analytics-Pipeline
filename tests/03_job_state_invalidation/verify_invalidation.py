"""Verify the invalidation flow end to end for one job:

1. snapshot end offsets of match_invalidation_events (segment start)
2. close one open job row in the SOURCE database
   (Debezium -> CDC -> ksql -> match_invalidation_events)
3. consume the new topic segment: >= 1 invalidation for that job_id
4. (only with --spark) start the Spark match engine, let its thin
   invalidation writer query commit, then read the Delta table
   streaming_lake/match_invalidations and require the job_id.

Prints OK/FAIL lines; exit 0 on all-pass.
Requires .venv-streams (confluent-kafka); --spark needs pyspark.
"""
import json
import os
import subprocess
import sys
import time

from confluent_kafka import Consumer, TopicPartition

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC_DSN = os.environ.get(
    "SOURCE_PG_DSN",
    "host=localhost port=5432 dbname=recruitment user=postgres password=postgres")
TOPIC = "match_invalidation_events"


def psql(sql, db_container="my_postgres", database="recruitment"):
    out = subprocess.run(
        ["docker", "exec", db_container, "psql", "-U", "postgres",
         "-d", database, "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=30)
    return out.stdout.strip()


def close_job():
    """Close one open job via SQL through docker exec (no psycopg2 needed)."""
    try:
        job_id = psql("""SELECT post_id FROM job_posts
                          WHERE lower(status) = 'open' ORDER BY post_id LIMIT 1""")
        if not job_id:
            print("FAIL no open job to close in source db")
            return None, False
        upd = psql(f"""UPDATE job_posts SET status='closed', lifecycle_stage=5,
                        updated_at=now(), version=version+1
                        WHERE post_id='{job_id}'""")
        if "UPDATE 1" not in upd:
            print(f"FAIL update had no effect: '{upd}'")
            return None, False
        print(f"OK closed job {job_id} in source db")
        return job_id, True
    except Exception as e:
        print(f"FAIL source-db update failed: {e}")
        return None, False


def end_offsets(topic):
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "offprobe"})
    out = {}
    meta = c.list_topics(topic).topics[topic]
    for p in range(len(meta.partitions)):
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        out[p] = hi
    c.close()
    return out


def consume_segment(before, wait=90, expect_id=None):
    c = Consumer({"bootstrap.servers": "localhost:9092",
                  "group.id": f"test03-{int(time.time())}",
                  "enable.auto.commit": False})
    tps = []
    want = 0
    for p, end in before.items():
        if end > 0:
            tps.append(TopicPartition(TOPIC, p, 0))
            want += end
    c.assign(tps)
    recs, hits, deadline = 0, 0, time.time() + wait
    samples = []
    grace_until = None  # quiet-grace after the full snapshot is consumed
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is not None and not m.error():
            recs += 1
            try:
                data = json.loads(m.value())
            except Exception:
                continue
            if len(samples) < 3:
                samples.append(data)
            # ksql emits UPPERCASE field names (identifier normalization)
            rec_job = (data.get("job_id") or data.get("JOB_ID"))
            if rec_job == expect_id:
                hits += 1
                break
            continue
        if recs >= want:
            # full snapshot consumed and nothing new this poll: give a
            # fresh event ~20s of quiet before concluding it never arrived
            if grace_until is None:
                grace_until = time.time() + 20
            elif time.time() > grace_until:
                break
    c.close()
    return recs, hits, samples


def spark_leg(job_id, before):
    """Spawn spark_leg_runner.py under the SYSTEM python (pyspark lives
    there, not in .venv-streams) and relay its verdict."""
    runner = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "spark_leg_runner.py")
    sys_py = "python"  # system python on PATH: the env with pyspark 3.5.3
    proc = subprocess.run(
        [sys_py, runner, job_id], cwd=REPO,
        capture_output=True, text=True, timeout=800)
    # relay the leg's significant lines
    for line in (proc.stdout or "").splitlines():
        if line.startswith(("HIT", "FAIL", "  waiting")):
            print(line)
    if proc.returncode != 0:
        tail = (proc.stderr or "").splitlines()[-3:]
        for line in tail:
            print("     " + line)
    return proc.returncode == 0


def main():
    with_spark = "--spark" in sys.argv
    before = end_offsets(TOPIC)
    job_id, closed = close_job()
    if not closed:
        print("FAIL")
        sys.exit(1)
    recs, hits, samples = consume_segment(before, expect_id=job_id)
    ok2 = recs > 0
    ok3 = hits > 0
    print(("OK " if ok2 else "FAIL ") + f"invalidation topic: {recs} new record(s)")
    print(("OK " if ok3 else "FAIL ") + f"specific invalidation for {job_id}: {hits} hit(s)")
    print("  sample:", json.dumps(samples[0]) if samples else "-")
    ok4 = True
    if with_spark:
        ok4 = spark_leg(job_id, before)
    print("PASS" if (ok2 and ok3 and ok4) else "FAIL")
    sys.exit(0 if (ok2 and ok3 and ok4) else 1)


if __name__ == "__main__":
    main()
