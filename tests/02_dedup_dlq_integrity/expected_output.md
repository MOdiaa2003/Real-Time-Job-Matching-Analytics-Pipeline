## tests/02_dedup_dlq_integrity - expected output

Injects real faults through the Avro producer and reconciles EVERY d record.

Procedure (run.ps1):
1. `reconcile_segment.py record` snapshots end offsets of
   `application_submitted_events` / `application_submitted_dedup` /
   `dlq.application_submitted_events`.
2. The producer runs 12s at 6 eps with `--duplicate-rate 0.3 --malformed-rate 0.15`
   (same fault injector used for demo load).
3. Wait until the Quix processor drains (offset stop growing, <=45s).
4. `reconcile_segment.py check` consumes exactly the new segment from all
   three topics and asserts:

| Check | Expected |
|-------|----------|
| dedup output unparsable | 0 - every output record is valid Confluent-wire Avro |
| DLQ parseable | 0 UNPARSEABLE records |
| malformed -> DLQ | input bad-wire count == DLQ `BAD_WIRE_HEADER`+`INVALID` count |
| replays -> DLQ | input replay count == DLQ `DUPLICATE` count |
| no silent loss | every event_id either appears in the output or is fully accounted in DLQ; per-id accounting `input-1 == (output-1) + dlq` |

The final printed line is the quantitative receipt, e.g. from the Stage-4 run:

```
PASS
  OK dedup output decodable records: output_unparsable=0
  OK DLQ records are parseable JSON (dlq_codes={'DUPLICATE': 30, 'BAD_WIRE_HEADER': 13})
  OK malformed input -> DLQ: input_bad_wire=13 dlq={'DUPLICATE': 30, 'BAD_WIRE_HEADER': 13}
  OK replayed copies -> DLQ: input_replays=30 dlq_duplicate=30
  OK no event lost without output/DLQ trace (missing=[])
  OK per-id accounting holds for all ids (first_mismatch=[])
summary: input_decodable=147 input_replays=30 input_bad_wire=13 output_rows=147 dlq={'DUPLICATE': 30, 'BAD_WIRE_HEADER': 13}
```
