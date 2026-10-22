## tests/06_latency_freshness - expected output

Measures the freshness SLA of business problem 4 on the REAL pipeline path
(web-event producer -> Quix dedup output), not a synthetic clock:

1. timestamp `start_ms`
2. produce 24 events at 4/s through `application_event_producer.py`
3. consume `application_submitted_dedup` from the beginning (fresh consumer
   group), keeping only records with `produced_at_ms >= start_ms`
4. latency per event = consume_time - produced_at_ms

| Check | Expected |
|-------|----------|
| events observed | every produced event seen on the output (24) |
| **p95 latency** | **<= 10000 ms (10-second freshness SLA)** |
| max latency | <= 15000 ms |

Expected console output (numbers from the actual run are recorded in
tests/RESULTS.md, placeholders here):

```
producer: DONE {'sent': 24, 'duplicated': 4, 'malformed': 2, ...}
measured: events=20 distinct=20 p50=412ms p95=1090ms max=1512ms (SLA target p95<= 10000ms)
PASS latency probe
```
