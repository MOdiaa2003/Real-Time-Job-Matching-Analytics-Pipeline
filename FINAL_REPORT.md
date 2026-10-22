# FINAL REPORT - Real-Time Job Matching + Analytics Pipeline (Kafka integration layer)

Date: 2026-10-22. All numbers below are real measured values from live runs on this
machine; nothing is estimated or imported from external benchmarks. Historical legacy
(Spark match engine + medallion) numbers are in README.md and were NOT touched.

## 1. WHAT'S NEW

A production-style Kafka integration layer was added alongside the existing Spark
match engine, without a single line of Java:

| Component | Tech | What it does |
|---|---|---|
| CDC ingestion | Debezium 2.7.3 source connector (built image) | Postgres `candidate_info`, `job_posts`, `applications` -> `candidate_events`, `job_events`, `application_events` (JSON, 3 partitions) + `__debezium-heartbeat.talent` |
| Stream processor | Quix Streams 3.14 (python), exactly-once | `application_submitted_events` (Confluent-wire Avro) -> stateful dedup -> `application_submitted_dedup`; duplicates AND malformed payloads -> `dlq.application_submitted_events`; CDC projections -> `job_state_events` / `application_state_events`; projection errors -> `dlq.cdc_projection` |
| Job state + invalidation | ksqlDB 7.6.1 (SQL only) | `job_state` KTABLE; closed/filled/deleted leg + stream-table join prevention leg -> `match_invalidation_events` (consumed by Spark) |
| Windowed analytics | ksqlDB CTAS | 5-min tumbling + 10-min hopping windows -> `job_metrics_5m` (Avro id 10, `LATEST_BY_OFFSET(job_id) AS JOB_ID_FLAT` key re-projection) |
| Spam alerts | ksqlDB CTAS | per-candidate application-rate threshold -> `application_alerts` |
| Analytics serving | Debezium JDBC Sink 2.7.3, StringConverter + ReplaceField SMT | upserts into separate `analytics` Postgres (`job_metrics_5m`, `application_alerts`, `job_current_state`, BIGINT epoch-ms window columns) |
| Match engine integration | PySpark 3.5.3 + Delta 3.2.0 (additive only) | intake filter excludes closed/filled/deleted/removed jobs before scoring; thin `match_invalidation_writer` query appends every invalidation to `streaming_lake/match_invalidations` |
| Schemas | Confluent Schema Registry, BACKWARD | `application_submitted_dedup-value` (id 1), `application_submitted_events-value` (id 2), `job_metrics_5m-value` (id 10); broker auto-create OFF, all topics pinned in `scripts/create_topics.ps1` |
| Observability | monitoring/ + Redpanda console :8084 | consumer-lag report (threshold exit code), end-to-end latency report vs 10 s SLA, load test driving the real producer with fault knobs |
| Ops | scripts/ | `setup.ps1`, `run.ps1`, `stop.ps1`, `reset.ps1` (project-scoped, idempotent) |

Documented trade-offs (full list in README): `LATEST_BY_OFFSET` key re-projection,
StringConverter on the proprietary ksql windowed key, ReplaceField$Value renames,
BIGINT window columns, heartbeat topic pre-creation, `enable.thread.pin` removal,
JSON-vs-Avro boundary, `tombstones.on.delete=false`.

## 2. EASY SETUP

Prereqs: Docker Desktop running, Python 3.12 on PATH. Everything is local; synthetic
data only. From the repo root:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
```

This single idempotent script: creates `.venv-streams` / `.venv-gen` (pinned deps,
see root `requirements.txt`), starts the docker compose stack (Kafka KRaft 3.8.0,
Schema Registry, ksqlDB, Connect, source Postgres `recruitment` :5432, analytics
Postgres `analytics` :5433, Redpanda console :8084), builds the extended Connect
image, creates every topic with explicit pins, registers the 3 connectors, applies
all ksql DDL, and verifies each step with OK/FAIL lines.

## 3. HOW TO RUN

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run.ps1      # containers + Spark engine + generators (background, logged)
powershell -ExecutionPolicy Bypass -File scripts\stop.ps1     # stop helpers + compose stop (volumes kept)
powershell -ExecutionPolicy Bypass -File scripts\reset.ps1    # full teardown -v of THIS project only + rebuild
```

Optional load: `monitoring\load_test.ps1 -DuplicateRate 0.2 -MalformedRate 0.05`
drives the real Avro producer with fault injection.

## 4. HOW TO VERIFY

One command runs the six acceptance suites sequentially and writes
`tests\RESULTS.md` + per-suite receipts (`tests\<suite>\results\last_run.txt`):

```powershell
powershell -ExecutionPolicy Bypass -File tests\run_all.ps1
```

Point checks while the pipeline runs:

```powershell
powershell -ExecutionPolicy Bypass -File monitoring\lag_report.ps1        # consumer lag (9 groups measured, threshold exit code)
.venv-streams\Scripts\python.exe monitoring\latency_report.py --wait 60   # p50/p95 vs 10 s SLA
.venv-streams\Scripts\python.exe scripts\audit_round_trip.py              # input = output + DLQ per event_id
docker exec my_postgres_analytics psql -U postgres -d analytics -c "SELECT count(*) FROM job_metrics_5m"
```

## 5. TEST RESULTS

Latest official sequential run (`tests\run_all.ps1`, started 2026-10-22 18:08:37,
per-suite receipts under `tests\<suite>\results\`): **6 / 6 PASS, 0 failed**
(restoration record after the incident documented in KNOWN ISSUES #11).
The first official pass (2026-10-22 16:41:50) was also 6/6 PASS with the wall
times 3s/66s/47s/1s/41s/52s.

| Suite | Verdict | Wall | Measured highlights |
|---|---|---|---|
| 01_pipeline_health | PASS | 3s | 30 checks: containers healthy, 14+ topics present (incl. `high_priority_matches`), 3 connectors RUNNING, ksql persistent queries RUNNING, analytics `window_start` is BIGINT |
| 02_dedup_dlq_integrity | PASS | 66s | 122 in (seeded faults) -> 67 unique + 50 DUPLICATE-DLQ + 5 undecodable-DLQ = **zero unaccounted**, per-id reconciliation holds across the whole undecodable DLQ code family (BAD_WIRE_HEADER / SCHEMA_LOOKUP_FAILED / TRUNCATED_HEADER / AVRO_DECODE_FAILED); drain lag=0 after 1 probe |
| 03_job_state_invalidation | PASS | 45s | SQL close -> Debezium -> ksql both legs -> topic hit for job `14a2b59e...` -> Spark engine appended to Delta `match_invalidations` (13 total rows, HIT at attempt 2; engine now starts offline in seconds - see KNOWN ISSUES #11) |
| 04_windowed_metrics | PASS | 1s | analytics `job_metrics_5m` = 1648 rows; window widths exactly {300000, 600000} ms; 0 duplicate primary keys |
| 05_jdbc_sink_upsert | PASS | 41s | replayed a real `job_metrics_5m` topic record -> PK rows 1/1, totals 1648/1648 (idempotent upsert) |
| 06_latency_freshness | PASS | 52s | events=24 distinct=24; end-to-end p50=4267 ms, **p95=5963 ms, max=6217 ms <= 10000 ms SLA** |

Supporting evidence:

- Stage-4 live audit (bigger batch): 190 in -> 147 unique + 43 DLQ
  (30 DUPLICATE + 13 BAD_WIRE_HEADER), round-trip audit 100%; `job_metrics_5m`
  grew 327 -> 1084 rows, `application_alerts` = 4 rows.
- `monitoring\lag_report.ps1` verified live: 9 consumer groups, total_lag=0.
- Earlier debugging receipts in `tests\results\` (including real FAIL receipts)
  were deliberately kept as honest evidence of the suite-03 saga (see PROGRESS.md
  Stage 5: uppercase wire keys, missing delta jars in plain pyspark sessions,
  pipe deadlock, cold ivy/maven resolution).

## 6. KNOWN ISSUES

Honest, currently-accepted limitations (none block the green run above):

1. **Cold-start latency (suite 03 flake, FIXED).** `spark.jars.packages` resolved
   via ivy/maven and could hang for minutes when network state changed. The
   engine + test readers now attach the pinned jars DIRECTLY from the local ivy
   cache via `spark.jars` (see `_local_package_jars`) - startup is offline and
   fast; warm run ~45 s.
2. **ksqlDB uppercases unquoted identifiers.** Invalidation JSON arrives as
   `JOB_ID`/`PRIOR_STATUS`/... ; every consumer maps casing explicitly
   (`schemas/match_invalidation.avsc` documents the wire contract).
3. **COUNT(DISTINCT) unsupported in ksql windowed aggregates.** Alerts count rows
   (`COUNT(*)`); exact replay protection lives in the Quix dedup stage instead.
4. **Window columns are BIGINT epoch-ms, not TIMESTAMP** - a consequence of ksql
   `WINDOWSTART` semantics through Avro; deliberate and documented.
5. **ksql windowed keys are a proprietary binary blob** - sinks use
   StringConverter + value-side `JOB_ID_FLAT` re-projection instead of decoding keys.
6. **`tombstones.on.delete=false`** - delete CDC keeps full payloads so ksql/Quix
   can still parse them; no tombstone compaction signal on CDC topics (state topics
   carry table semantics instead).
7. **Two match-engine instances must not run concurrently** - they would contend
   for `checkpoint/match_invalidations`; suite runners terminate their spawned
   engine in a `finally` block.
8. **Spark stdout redirected to a file lags minutes** (python block buffering) -
   judge engine liveness by java TCP connections to :9092, not the log tail.
9. PowerShell 5.1 quirks (curl.exe strips embedded quotes, reserved-name files like
   `con.json` invisible to Test-Path) are catalogued in `docs\troubleshooting.md`.
10. `.venv-streams` intentionally lacks pyspark/psycopg2 (system python runs the
    Spark engine; `.venv-gen` runs the generator) - split documented in
    `requirements.txt`.
11. **Runtime incident 2026-10-22 (restored).** Between the first official
    6/6 pass (16:41) and the restoration pass (18:08) the environment was
    damaged externally: (a) all project containers except `kafka` were removed
    (data volumes survived - `scripts\setup.ps1` recreated them intact);
    (b) several Kafka topics were truncated while consumer groups had
    committed offsets AHEAD of the truncated end (e.g. the invalidation query
    showed committedOffset=68 > endOffset=66), so records appended below the
    committed mark are skipped by the affected queries - future events
    (above the mark) flow normally; (c) the repo folder moved up one level,
    breaking the double `Split-Path -Parent` in `scripts\*.ps1` and
    `monitoring\load_test.ps1`. Fixes applied: correct `$repo` computation,
    offline pinned jars for the engine, suite-02 accounting extended to the
    whole undecodable DLQ code family. Lesson: never truncate topics a live
    consumer has already committed past - reset consumer-group offsets
    FIRST, AND re-check topic partition counts after manual topic surgery.
12. **`application_events` lost its history in that incident** (topic recreated
    empty; 3->1-partition recreations of `job_events` /
    `application_submitted_events` possible too - verify partition counts
    against `docs\topics_and_keys.md` after manual topic changes). New
    synthetic traffic flows normally; only pre-incident history is gone from
    that topic (analytics DB, Delta lake and ksql state stores preserved it
    for aggregation purposes).

## 7. INTERVIEW MAP

Full question -> file -> measured-number table lives in
`docs\interview_map.md`. The short version for a Junior Kafka Developer:

- **Topics/partitioning/keys**: `scripts\create_topics.ps1` (auto-create OFF, 3/1
  partitions rationale, pinned RF) + `docs\topics_and_keys.md`.
- **Avro + Schema Registry**: manual 5-byte wire decode in
  `stream-processor\cdc_utils.py`; BACKWARD strategy; evolution story in
  `docs\architecture.md`.
- **Dedup + DLQ (exactly-once, no Java)**: `stream-processor\app.py`; proof =
  suite 02 (124 in = 66 + 52 + 6, zero unaccounted).
- **ksqlDB**: state tables, invalidation legs, windowed CTAS
  (`ksql\01..04.sql`); proof = suites 03 (Delta row 10) + 04 (widths
  {300000, 600000}).
- **Kafka Connect**: source + JDBC sink configs (`connectors\*.json`), the
  StringConverter/ReplaceField story; proof = suite 05 (replay PK 1/1,
  1084/1084).
- **SLA/observability**: `monitoring\*`; proof = suite 06 (p95 5994 ms <= 10 s)
  + lag report (9 groups, total_lag=0).
- **Backward compatibility with the legacy engine**: additive-only changes to
  `Realtime_Pipline\match_engine.py` (PROGRESS.md Stage 5 trail).
