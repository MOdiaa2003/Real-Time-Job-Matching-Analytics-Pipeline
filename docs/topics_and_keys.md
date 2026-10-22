# Topics, keys, and schema contracts

Every topic has a PARTITION COUNT and retention set explicitly in
`scripts/create_topics.ps1` (broker auto-create is OFF on purpose: a
mis-keyed write can never create a wrong-layout topic).

## Business-data topics (source CDC, JSON)

| Topic | Partitions | Key | Notes |
|-------|------------|-----|-------|
| `candidate_events` | 3 | candidate_id | Debezium CDC out; JSON value, op c/u/d/r |
| `job_events` | 3 | post_id | plus `__debezium-heartbeat.talent` (1 part, auto-created by Debezium's heartbeat; pre-created in setup so monitor tools never see a race) |
| `application_events` | 3 | application_id | uncompressed by the source connector |

## Stream-processor (Quix Streams) topics

| Topic | Partitions | Key | Format | Purpose |
|-------|-----------|-----|--------|---------|
| `application_submitted_events` | 3 | job_id | Confluent-wire Avro (`application_submitted_events-value`, BACKWARD) | input of the dedup; injected DETERMINISTICALLY from `Realtime_Pipline/application_event_producer.py` (idempotent producer) |
| `application_submitted_dedup` | 3 | job_id | Confluent-wire Avro (`application_submitted_dedup-value`) | exactly-once output, first occurrence per event_id within state retention |
| `dlq.application_submitted_events` | 1 | original key | JSON | records: `{error_code, event{...}, details}` - `DUPLICATE`, `BAD_WIRE_HEADER`, ... (duplicates AND invalid payloads both land here so every input has exactly one verdict) |
| `dlq.cdc_projection` | 1 | - | JSON | CDC rows whose projection to state events failed |
| `job_state_events` | 3 | post_id | JSON, COMPACTED | ksql job_state KTABLE source |
| `application_state_events` | 3 | application_id | JSON, COMPACTED | ksql application KTABLE source |

## ksqlDB outputs

| Topic | Partitions | Key | Format | Notes |
|-------|-----------|-----|--------|-------|
| `match_invalidation_events` | 3 | job_id | JSON (UPPERCASE keys on wire - ksql identifier normalization) | fed by BOTH legs (closed/filled/deleted transitions + prevention join); consumed by Spark thin writer + `dlq`-free |
| `application_alerts` | 1 | candidate key | Confluent-wire Avro (ksql owns schema id) | spam/anomaly candidates |
| `job_metrics_5m` | 3 | windowed job key (proprietary ksql key blob) | Confluent-wire Avro (id 10) | window values are re-projected into the VALUE via `LATEST_BY_OFFSET(...) AS *_FLAT` because GROUP BY keys are NOT repeated in windowed-table values; sink uses StringConverter for the binary key |
| `APPLICATIONS_BY_JOB`, `APP_DEDUP_WITH_JOB_STATE` | 3 | - | JSON | ksql materialization helper topics (internally owned) |

## Sink topic contract notes

* `__debezium-heartbeat.talent` - Debezium's heartbeat topic for the source connector (1 partition). It is explicitly pre-created in setup so monitors never observe a transient missing topic between the connector starting and its first heartbeat.
* `tombstones.on.delete=false` on the source connector means `op=d` arrives with a FULL payload instead of a null tombstone - chosen so ksql/Quix keep parsing the `before` value even on deletes; trade-off: no tombstone-driven compaction signal on the CDC topics, the COMPACTED state topics carry the table semantics instead.

## Key schemas (registered, BACKWARD compat)

* `application_submitted_events-value` (id 2) / `application_submitted_dedup-value` (id 1)
* `job_metrics_5m-value` (id 10, v2), `application_alerts-value` (ksql-owned)
* `schemas/match_invalidation.avsc` - schema of record (documented but NOT enforced on the topic: the topic is JSON by design)

## Partition defaults and why

Explicit pins in `create_topics.ps1`: business traffic = 3 partitions
(scales 3 consumer instances, local-demo friendly), DLQ/table materializations
= 1 partition (diagnostic or low cardinality streams; ordering for humans).
Change with `TOPIC_PARTITIONS` env var before setup if you want more
parallelism.
