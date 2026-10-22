## tests/03_job_state_invalidation - expected output

Closes a real job row in the source PostgreSQL and follows the invalidation
all the way into the Spark lake:

| Step | What should happen |
|------|--------------------|
| 1 UPDATE job_posts SET status='closed' | Debezium captures the row change |
| 2CDC -> Quix -> job_state_events | ksql job_state KTable updated |
| 3 ksql 02_job_state_invalidation.sql leg | `match_invalidation_events` receives a JSON record with UPPERCASE keys `{JOB_ID:'JOB_000123', PRIOR_STATUS:'open', REASON:'job_closed', INVALIDATION_TIME_MS:..., SOURCE_OP:'u'}` (ksqlDB normalizes unquoted identifiers) |
| 4 Spark match engine (thin writer query) | `streaming_lake/match_invalidations` Delta table gains a row for that `job_id` |

Expected console output (values abbreviated):

```
OK closed job JOB_000123 in source db
OK invalidation topic: 3 new record(s)
OK specific invalidation for JOB_000123: 1 hit(s)
  sample: {"job_id": "JOB_000123", "prior_status": "open", "reason": "job_closed", ...}
OK Delta match_invalidations contains the job:
     job_id=JOB_000123 reason=job_closed prior_status=open source_op=u
PASS
```
