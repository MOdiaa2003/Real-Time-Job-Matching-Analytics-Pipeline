# Interview map - where every talking point lives in code

For a Junior Kafka Developer interview: each question area, the exact file
that proves it, and the measured result to quote.

## Kafka fundamentals

| Question | Show this | Requirement it satisfies |
|----------|-----------|--------------------------|
| "How do you choose partition count?" | `scripts/create_topics.ps1` (explicit pins, auto-create OFF, 3 partitions business / 1 DLQ, with per-topic rationale in comments) | business problem 8 |
| "How does ordering + keys work?" | producers key by entity id (`job_id`/`post_id`/`application_id`): one ordering domain per entity; windowed topics keyed by the ksql window blob | dedup + upsert correctness |
| "idempotent vs exactly-once?" | producer `enable.idempotence=true` + Quix exactly-once processing w/ changelogs (`stream-processor/app.py`) | business problem 7 |
| "How do you pin replication factor in a single-node demo?" | RF=1 everywhere; rationale in create_topics.ps1 comments + how production would pin RF=3 with min.insync.replicas=2 | n/a (demo) |

## Schema Registry & Avro

| Question | Show this | Notes |
|----------|-----------|-------|
| "Confluent wire format" | `stream-processor/cdc_utils.py` manual 5-byte wire decode (magic byte + schema id + fastavro schemaless_reader) | no-rest-lookup per record: schemas cached by id |
| "evolution strategy" | Registry shows BACKWARD; `schemas/breaking_change_example.avsc` documents a rejected-breaking change + how it would be handled (new subject / full redeploy) | honest trade-off story |
| "what if registry is down?" | producer fails fast (no unregistered writes); consumers with cached schema ids keep working until restart; recovery plan in docs/architecture.md | n/a |

## Stream processing WITHOUT Java

| Question | Show this |
|----------|-----------|
| "dedup design" | `stream-processor/app.py`: state store keyed event_id with TTL > duplicate window; retention pruning BEFORE duplicate check (so reads never see pruned-but-dup state); first-occurrence forwarded, replays DLQ'd |
| "poison message handling" | DLQ with `error_code` taxonomy (`BAD_WIRE_HEADER`, `DUPLICATE`, `INVALID`); JSON DLQ payloads keep the original event for forensics |
| "CDC projections" | `job_state_events`/`application_state_events` built by Quix from raw CDC; failures go to `dlq.cdc_projection` |

## Kafka Connect

| Question | Show this |
|----------|-----------|
| "source vs sink configs" | `connectors/postgres-source.json` (JOINS heartbeat, tombstone trade-off) and `connectors/jdbc-sink-*.json` (upsert PK definition, StringConverter on the proprietary ksql key, ReplaceField$Value renames, quote.identifiers=false) |
| "why StringConverter here?" | ksql windowed keys are a binary blob, not JSON (docs/troubleshooting.md #3) |
| "how do you avoid double counting on sink replay?" | tests/05: replayed a real topic record - PK row count stayed 1, totals unchanged |

## ksqlDB

| Question | Show this |
|----------|-----------|
| "table vs stream" | `ksql/01..04`: KTABLEs for state (compacted topics), STREAMs for transitions |
| "windowed aggregation end-to-end" | `ksql/03_windowed_metrics.sql` 5m TUMBLING + 10m HOPPING CTAS with grace periods -> tests/04 validates window widths {300000, 600000} ms |
| "stream-table join for invalidation" | `ksql/02` prevention leg: application lands on closed job -> invalidation |
| "ksql gotchas you hit" | LATEST_BY_OFFSET re-projection trick, COUNT(DISTINCT) unsupported, windowed CTAS restrictions, UPPERCASE identifier normalization (docs/troubleshooting.md #4) |

## Reliability / observability

| Question | Show this | Measured |
|----------|-----------|----------|
| "how do you prove no data loss?" | `tests/02` per-id reconciliation: 116 in -> 60 unique + 44 DUPLICATE DLQ + 12 BAD_WIRE_HEADER DLQ = 0 unaccounted | real output in tests/02 results |
| "freshness SLA" | `tests/06` (p50 4745ms, p95 5996ms, max 6247ms <= 10000ms SLA) | tests/06 results |
| "consumer lag monitoring" | `monitoring/lag_report.ps1` (per-group lag + threshold exit + history jsonl) | runs on demand |
| "load testing" | `monitoring/load_test.ps1` (rate/duration/dup/malformed knobs through the REAL producer) | - |
| "one-command ops" | `scripts/setup.ps1`, `run.ps1`, `stop.ps1`, `reset.ps1` (idempotent, project-scoped) | - |

## Spark side (the pre-existing engine, extended additively)

| Question | Show this |
|----------|-----------|
| "how did you keep the old pipeline working?" | additive changes only: intake filter, thin invalidation writer, absolute-root defaults; everything else untouched (PROGRESS.md trail) |
| "point-in-time correctness of matches" | every scored row carries the job status AT SCORING TIME; invalidation events are appended to Delta so replay/audit can reproduce any moment |
| "batch metrics" | `batch_metrics.jsonl` per batch (timings, rows, shuffle) - used for perf work, kept in Delta lake dir |

## Automation testing strategy

| Question | Show this |
|----------|-----------|
| "what makes a test WRONG pass?" | tests/02 originally PASSED with a fake PASS due to a stale $LASTEXITCODE - fixed by explicit rc checks; the gotcha is documented (docs/troubleshooting.md #7) |
| "test determinism" | seeded producer, recorded offset segments (record/check two-phase), specific state assertions, no sleeps-only tests |
| "running everything" | `tests/run_all.ps1` -> sequential suites + tests/RESULTS.md verdicts; each suite writes timestamped `results/*.txt` receipts |
