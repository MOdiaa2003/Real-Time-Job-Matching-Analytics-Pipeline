## tests/05_jdbc_sink_upsert - expected output

Proves the Debezium JDBC sink writes are REPLAY-SAFE (exactly the guarantee
we rely on after a sink task restart, since Connect offsets can lag):

1. Reads the latest `job_metrics_5m` record (raw bytes from Kafka).
2. Snapshots the analytics-DB row count for that primary key
   `(job_id, window_start, window_end)` and the table total.
3. Re-produces the SAME record to the topic (simulating a replay).
4. Waits for the sink, then re-checks:

| Check | Expected |
|-------|----------|
| PK row count | still exactly 1 row for that PK (no duplicate insert) |
| table total | unchanged (no appended dup) |
| row content | `applications_cnt/matches_cnt` equal the replayed record's values (recomputed, not summed) |

Expected console output:

```
OK replaying record for JOB_000123 window[1792668000000..1792668600000] apps=2 matches=0
OK record re-produced
OK PK rows before/after: 1/1; total: 327/327; row now: 2/0
PASS
```
