# Real-Time Candidate ↔ Job Matching Pipeline

An end-to-end data engineering project that simulates a recruitment platform and scores every job application in near real time.
Synthetic recruitment activity is written to PostgreSQL, captured as CDC events by Debezium, streamed through Kafka (Redpanda), and scored by a Spark Structured Streaming engine that writes to Delta Lake.

```
Generator ──► PostgreSQL ──► WAL ──► Debezium ──► Kafka (Redpanda) ──► Spark Match Engine ──► Delta Lake
 (Python)     (logical        (pgoutput)  (Kafka      candidate_events      (Structured         candidate_dim, job_dim,
              replication)                 Connect)    job_events            Streaming)          scored results, pending,
                                                       application_events                        dead-letter
                                                                                      └──► HIGH_PRIORITY matches → Kafka
```

**Stack:** Python · PostgreSQL 17 · Debezium 2.7 · Redpanda (Kafka API) · Apache Spark (PySpark Structured Streaming) · Delta Lake · Qdrant · Docker Compose

---

## Table of Contents

1. [Architecture](#architecture)
2. [Repository Structure](#repository-structure)
3. [Components](#components)
4. [Quick Start](#quick-start)
5. [Configuration](#configuration)
6. [Matching Logic](#matching-logic)
7. [Performance Engineering](#performance-engineering)
8. [Measured Results](#measured-results)
9. [Reliability Fixes](#reliability-fixes)
10. [Known Limitations & Roadmap](#known-limitations--roadmap)

---

## Architecture

| Stage | Responsibility |
|---|---|
| **1. Generator** | Pure PostgreSQL writer. Produces candidates, job posts and applications and moves them through realistic lifecycles. It never touches Kafka. |
| **2. PostgreSQL + WAL** | `wal_level=logical`; the three tables use `REPLICA IDENTITY FULL` and carry `created_at` / `updated_at` / `version` columns so Debezium can emit full before/after row images. |
| **3. Debezium → Kafka** | A Postgres source connector reads the WAL via `pgoutput` and publishes to `candidate_events`, `job_events` and `application_events` (renamed with `RegexRouter` transforms). |
| **4. Match engine** | Spark Structured Streaming job (30 s trigger). Upserts candidate/job dimensions into Delta, enriches new and pending applications, scores them, and routes the outcome. |
| **5. Output** | Scored results in Delta, unresolved applications in a pending table (retried up to 5 times), then a dead-letter table. `HIGH_PRIORITY` matches are published to Kafka. |

Keeping CDC out of application code means the generator stays simple and change capture runs asynchronously downstream.

---

## Repository Structure

```
.
├── docker-compose.yml                  # Postgres, Redpanda, Redpanda Console, Kafka Connect (Debezium), Qdrant
├── recruitment_lifecycle_generator.py  # Synthetic data generator with lifecycle management
├── debezuim_connector.py               # Registers / inspects / deletes the Debezium connector via REST
├── match_engine.py                     # Spark Structured Streaming matching engine (+ process_application_batch)
├── data_used.py                        # Reference lookup tables (titles, skills, salary ranges, ...)
├── pipeline_improvements_report.md     # Detailed improvements report
└── recruitment_pipeline_performance_improvements_report_quantified.pdf
```

> `data_used.py` is imported by the generator; include it in the repo.
> Rename `debezuim_connector.py` → `debezium_connector.py` if you want the typo gone.

---

## Components

### Data generator — `recruitment_lifecycle_generator.py`

Randomly (weighted) chooses an entity (candidate, job post, application) and an operation (insert / update / delete), while respecting lifecycles:

- **Candidate:** active → availability changes → profile deactivated
- **Job:** created → salary / deadline / skills / work-mode changes → closed
- **Application:** applied → viewed → interview → offer → rejected | accepted

Key design points:

- **Intelligent applications:** 70 % category-matched, 30 % random. Category-matched picks only draw from entities that have finished CDC warm-up.
- **CDC warm-up delay:** a new candidate/job waits 60 s (longer than the 30 s micro-batch trigger) before it can be referenced by an application. This removes the "application arrives before its candidate/job reached the dimension tables" problem (`unresolved_rows`).
- **Internally consistent records:** candidate careers are built from dated role progressions (category → seniority → title → experience → skills → salary → companies); jobs follow company → industry → category → title → seniority → skills → salary → location, with cost-of-living salary adjustment.
- **Realistic timing:** application delay buckets (0–2 d 35 %, 3–7 d 35 %, 8–14 d 20 %, 15+ d 10 %) with deadline decay; 72 % of applications are eventually viewed.
- **Performance:** one persistent DB connection, in-memory lifecycle pools hydrated once at startup, workload-weighted operation mix (default `total_operations = 1,000,000`).

### CDC connector — `debezuim_connector.py`

Idempotent registration of the Debezium PostgreSQL connector through the Kafka Connect REST API (`PUT /connectors/<name>/config`).

```bash
python debezuim_connector.py register   # create or update
python debezuim_connector.py status
python debezuim_connector.py delete
```

Captured tables: `public.candidate_info`, `public.job_posts`, `public.applications` → topics `candidate_events`, `job_events`, `application_events`.

### Match engine — `match_engine.py`

- Parses Debezium envelopes with explicit `StructType` schemas (no schema inference).
- Maintains `candidate_dim` and `job_dim` Delta tables using `MERGE`, de-duplicated by key and highest `version`.
- `process_application_batch()` (the `foreachBatch` handler) combines new and pending applications, enriches them with the dimensions, scores them natively in Spark SQL, and writes results, pending rows, dead-letter rows and `HIGH_PRIORITY` Kafka events.
- Writes per-batch metrics (duration, rows/sec, row and file counts, shuffle, broadcast size, stage timings) to `batch_metrics.jsonl`.

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ with `pyspark`, `delta-spark`, `psycopg2`, `numpy`, `requests`, `python-dotenv`
- Java 11 or 17 (for Spark)

### 1. Create a `.env` file

```env
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# Redpanda
REDPANDA_IMAGE_TAG=v24.2.7
REDPANDA_CONTAINER_NAME=redpanda
REDPANDA_NODE_ID=0
KAFKA_EXTERNAL_PORT=9092

# Debezium / Kafka Connect
KAFKA_CONNECT_URL=http://localhost:8083
DEBEZIUM_DB_HOST=postgres          # Docker service name, NOT localhost

# Spark
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
CHECKPOINT_ROOT=./data/checkpoint
DELTA_ROOT=./data/streaming_lake
SPARK_LOCAL_CORES=2
```

> `REDPANDA_IMAGE_TAG` above is just an example; use a Redpanda version you have tested.

### 2. Start the infrastructure

```bash
docker compose up -d
```

| Service | URL / Port |
|---|---|
| PostgreSQL | `localhost:5432` |
| Redpanda (Kafka API) | `localhost:${KAFKA_EXTERNAL_PORT}` |
| Redpanda Console | http://localhost:8084 |
| Kafka Connect REST | http://localhost:8083 |
| Qdrant | http://localhost:6333 (REST), `6334` (gRPC) |

### 3. Create the schema

Run the generator once so it creates the tables, sets `REPLICA IDENTITY FULL`, and starts producing rows. The Debezium connector expects those tables to exist.

```bash
python recruitment_lifecycle_generator.py
```

### 4. Register the Debezium connector

```bash
python debezuim_connector.py register
python debezuim_connector.py status    # connector and task should be RUNNING
```

### 5. Start the match engine

```bash
spark-submit \
  --packages io.delta:delta-spark_2.12:<version>,org.apache.spark:spark-sql-kafka-0-10_2.12:<spark-version> \
  match_engine.py
```

Pick package versions that match your Spark/Scala build.

### 6. Watch it work

Open Redpanda Console to inspect the CDC topics and `high_priority_matches`. Read the metrics log at `${DELTA_ROOT}/_metrics/batch_metrics.jsonl`.

---

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_*` | `postgres` / `localhost:5432` | Generator DB connection |
| `DEBEZIUM_DB_HOST` | `postgres` | Hostname as seen **inside** the Kafka Connect container |
| `DEBEZIUM_CONNECTOR_NAME` | `candidate-job-matching-cdc` | Connector name |
| `DEBEZIUM_TOPIC_PREFIX` | `talent` | Debezium topic prefix |
| `DEBEZIUM_SLOT_NAME` | `talent_cdc_slot` | Replication slot name |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Spark → Kafka |
| `CHECKPOINT_ROOT` / `DELTA_ROOT` | local paths | Streaming checkpoints / Delta lake root |
| `SPARK_LOCAL_CORES` | `2` | Local parallelism (kept low on Windows) |
| `SPARK_PARQUET_CODEC` | `snappy` | Set to `uncompressed` to avoid the Windows Snappy DLL issue |
| `METRICS_LOG_PATH` | `${DELTA_ROOT}/_metrics/batch_metrics.jsonl` | Per-batch metrics output |

The default `CHECKPOINT_ROOT` and `DELTA_ROOT` in `match_engine.py` point to a local Windows path. Override them for your machine.

---

## Matching Logic

Each application is scored from 0 to 100 across five components, then combined with fixed weights:

| Component | Weight |
|---|---|
| Skills | 30 % |
| Experience | 25 % |
| Location | 20 % |
| Salary | 15 % |
| Language | 10 % |

Priority tiers: **HIGH_PRIORITY** ≥ 80, **MEDIUM_PRIORITY** ≥ 65, otherwise low.
All scoring is expressed in native Spark SQL functions (no Python UDFs).

Applications whose candidate or job is not yet in the dimension tables are kept as **pending** and retried up to `MAX_ENRICHMENT_RETRIES = 5` times, then moved to a **dead-letter** table with a reason (`candidate_not_found`, `job_not_found`, or both).

---

## Performance Engineering

Most of the gains come from removing repeated work in the Spark path, not from changing the scoring rules.

| Optimization | Effect |
|---|---|
| Native Spark SQL scoring instead of Python UDFs | Removes the JVM ↔ Python worker round-trip per row |
| Cache `incoming`, `resolved`, `scored` DataFrames | Stops the same join/filter/score lineage from running twice |
| Broadcast joins on `candidate_dim` / `job_dim` | Avoids a shuffle join against the large application stream |
| Column pruning before broadcast and cache | Smaller payloads and less I/O (7 candidate + 10 job columns) |
| `isEmpty()` and Delta `operationMetrics` instead of `count()` | Fewer extra Spark jobs per batch |
| Dedupe CDC rows by key/version before `MERGE` | Less merge input and no multiple-source-row failures |
| Early exit on empty batches | Skips dimension reads, joins and writes |
| Single `_finish()` that unpersists every cached DataFrame | No cache leaks across batches |
| Delta `optimizeWrite` / `autoCompact` via `properties.defaults.*` | Mitigates small files (validate with `files_written`) |
| Generator: persistent connection + in-memory lifecycle state | Removes per-event connect cost and repeated table scans |

---

## Measured Results

Numbers come from the project's own `batch_metrics.jsonl` logs. They are observations from single runs, **not controlled before/after benchmarks**.

**Throughput after the initial replay (25 Aug run)**

| Batch | Incoming rows | Batch time | Rows/sec |
|---|---|---|---|
| 0 (initial replay) | 46,913 | 85.2 s | 551 |
| 1 | 17,518 | 39.0 s | 449 |
| 2 | 15,395 | 29.4 s | 524 |
| 3 | 13,488 | 25.5 s | 529 |

Batches 1–3 average roughly 500 rows/sec against a 30-second trigger. Batch 0 is dominated by the earliest-offset backlog and dimension upserts.

**Effect of the CDC warm-up delay (separate run)**

- `unresolved_rows` held at about 930 per batch, then fell to 43 and 30 (about 95 % lower) once warm-up-gated entities flowed through.
- `dead_letter_rows` spiked once (891) when the initial backlog exhausted its retries, then dropped to 16 and 1.

**Effect of caching and `isEmpty()`**

- `high_priority_check_sec` fell from 2.9 s (batch 0) to about 0.075 s in steady state, and stayed flat while `scored_rows` varied from roughly 700 to 2,000.
- `resolved_check_sec` stayed within 0.3–0.9 s while `resolved_rows` varied by more than 22×.

**Broadcast sizes:** candidate dimension about 0.0–0.44 MB, job dimension about 0.6–1.0 MB, far below Spark's 10 MB auto-broadcast default.

The largest remaining repeated cost is the **pending-table write** (about 7.5–19.6 s per batch in the 25 Aug run).

---

## Reliability Fixes

Most of these were found while running on Windows:

- **Pending-application crash:** `spark.createDataFrame([], schema)` spawned Python workers even for zero rows and caused `Python worker exited unexpectedly`. Tables are now pre-created with the `DeltaTable` API, and no empty-DataFrame fallback is needed.
- **Delta `MERGE` ambiguity:** `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` on the initial backlog is fixed by keeping one row per key (highest `version`) before merging.
- **Pinned Python interpreter:** `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` are set to `sys.executable` before the session is created.
- **Limited local concurrency:** `local[2]` plus `spark.python.worker.reuse=true`, because Windows has no `fork()`.
- **Snappy DLL issue:** Parquet codec can be set to `uncompressed` to bypass the native library locked by antivirus scans.
- **Task retries:** `spark.task.maxFailures=4`.
- **Immediate-flush step logging** (`_step_log`) so a crash can't swallow the last completed stage.

---

## Known Limitations & Roadmap

**Current limitations**

- The generator issues one `INSERT`/`UPDATE` + `commit()` per operation. Batched inserts (`execute_values` / `COPY`) or grouped commits are the clearest remaining generator-side speedup.
- Many lifecycle updates do a `SELECT → UPDATE → SELECT`; `UPDATE ... RETURNING` would save round trips.
- Full dimension snapshots are re-read on every application batch.
- Broadcast has no size guard yet; add one as dimensions grow.
- Pending-table persistence is the biggest per-batch cost in the logs.
- Parquet compression is disabled on Windows (larger files).
- The 30 s trigger is a stability choice, not a speed-up; tune it only after batch time is consistently below target.
- `HIGH_PRIORITY` volume depends on score calibration and on how realistic the generated candidate/job skill overlap is. Zero high-priority rows is a data/threshold question, not evidence of slow processing.

**Roadmap**

- Controlled benchmark protocol: identical workload, at least 3 warm runs, compare median `batch_duration_sec` and `input_rows_per_sec`, and track `files_read`, `files_written` and shuffle MB. Separate initial replay from steady state.
- Proposed (not yet measured) throughput target: 600–800 rows/sec after generator batching and pending-write optimization.
- **Recruitment Intelligence Assistant:** an agentic RAG layer on top of the deterministic engine, combining Delta data with CV / job-description documents embedded into Qdrant, plus SQL and other tools for multi-step root-cause questions.

---

## Documentation

- [`pipeline_improvements_report.md`](pipeline_improvements_report.md): every improvement in the generator and match engine, with measured numbers
- [`recruitment_pipeline_performance_improvements_report_quantified.pdf`](recruitment_pipeline_performance_improvements_report_quantified.pdf): performance report with impact ranking and metrics guide

## License

Add a license of your choice (for example MIT).
