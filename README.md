# Real-Time Candidate–Job Matching & Analytics Pipeline

An end-to-end recruitment data platform using PostgreSQL CDC, Debezium, Kafka, Spark Structured Streaming, Delta Lake, and a Bronze–Silver–Gold analytics pipeline. A Kafka integration layer adds Avro event deduplication, job-state invalidation, windowed recruiter metrics, application-rate alerts, and JDBC sinks. A read-only Streamlit dashboard surfaces matching results, pipeline metrics, and service health.

> **Status:** Core CDC, streaming matching, Kafka integration, medallion processing, and observability components are implemented. The Qdrant-based recruitment intelligence / RAG assistant is in progress.

## Architecture at a glance

<p align="center">
  <img src="./Real-Time Candidate Matching Pipeline.png" width="100%" />
</p>

## Streamlit Dashboard Preview

The Streamlit app contains six views for exploring matching results and monitoring the pipeline. The gallery below is wired to screenshots stored in `docs/images/streamlit/`.

> **Add real screenshots:** The dashboard files contain the Streamlit code but no screenshots. Capture the running app and save each image using the exact filename below; GitHub will render the gallery after those image files are committed.

<p align="center">
  <a href="./docs/images/streamlit/01-executive-overview.png">
    <img src="./docs/images/streamlit/01-executive-overview.png" alt="Streamlit Executive Overview showing pipeline KPIs and service status" width="100%" />
  </a>
  <br />
  <strong>Executive Overview</strong><br />
  <sub>Pipeline KPIs, processing paths, and component health at a glance.</sub>
</p>

<table>
  <tr>
    <td align="center" width="50%">
      <a href="./docs/images/streamlit/02-matching-explorer.png">
        <img src="./docs/images/streamlit/02-matching-explorer.png" alt="Streamlit Candidate–Job Matching Explorer with ranked candidates and match scores" width="100%" />
      </a><br />
      <strong>Candidate–Job Matching</strong><br />
      <sub>Ranked candidates, matching scores, and score breakdowns.</sub>
    </td>
    <td align="center" width="50%">
      <a href="./docs/images/streamlit/03-kafka-streaming-reliability.png">
        <img src="./docs/images/streamlit/03-kafka-streaming-reliability.png" alt="Streamlit Kafka Streaming and Reliability view with topic activity, consumer lag, and dead-letter indicators" width="100%" />
      </a><br />
      <strong>Kafka Streaming &amp; Reliability</strong><br />
      <sub>Topic activity, consumer lag, dead-letter signals, and sink freshness.</sub>
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <a href="./docs/images/streamlit/04-batch-medallion-quality.png">
        <img src="./docs/images/streamlit/04-batch-medallion-quality.png" alt="Streamlit Batch Medallion and Quality view for Bronze Silver and Gold outputs" width="100%" />
      </a><br />
      <strong>Batch Medallion &amp; Quality</strong><br />
      <sub>Bronze/Silver/Gold output status and data-quality indicators.</sub>
    </td>
    <td align="center" width="50%">
      <a href="docs/images/engine-performance-2.png">
        <img src="./docs/images/streamlit/05-engine-performance.png" alt="Streamlit Engine Performance view with batch duration, throughput, row counts, and stage timing charts" width="100%" />
      </a><br />
      <strong>Engine Performance</strong><br />
      <sub>Batch duration, input throughput, processing outcomes, and stage costs.</sub>
    </td>
  </tr>
  <tr>
    <td align="center" width="50%">
      <a href="./docs/images/streamlit/06-testing-health.png">
        <img src="./docs/images/streamlit/06-testing-health.png" alt="Streamlit Testing and Health view with Docker service probes, connector status, and saved acceptance test receipts" width="100%" />
      </a><br />
      <strong>Testing &amp; Health</strong><br />
      <sub>Service probes, connector/ksqlDB status, and saved acceptance-test receipts.</sub>
    </td>
    <td align="center" width="50%">
      <strong>Suggested extra chart capture</strong><br />
      <sub>For a more visual portfolio, add a close-up of the performance charts or the matching-score breakdown after the six main screenshots are in place.</sub>
    </td>
  </tr>
</table>

## Contents

- [Streamlit Dashboard Preview](#streamlit-dashboard-preview)
- [Goals and architecture](#goals-and-architecture)
- [Technology stack](#technology-stack)
- [Data flow and Kafka topics](#data-flow-and-kafka-topics)
- [Streaming matching and scoring](#streaming-matching-and-scoring)
- [Reliability and dead-letter handling](#reliability-and-dead-letter-handling)
- [Bronze–Silver–Gold pipeline](#bronze--silver--gold-pipeline)
- [Performance engineering and results](#performance-engineering-and-results)
- [Streamlit dashboard](#streamlit-dashboard)
- [Acceptance tests and local setup](#acceptance-tests-and-local-setup)
- [Limitations and roadmap](#limitations-and-roadmap)

## Goals and architecture


The system simulates recruitment activity and processes the resulting database changes through independent paths.

1. A Python generator creates candidate, job, and application lifecycle records in PostgreSQL.
2. Debezium reads committed changes from the PostgreSQL write-ahead log (WAL) and publishes them to Kafka.
3. Spark Structured Streaming enriches applications, calculates weighted scores, writes Delta results, and publishes high-priority matches.
4. Quix Streams deduplicates Avro submission events; ksqlDB handles job state, invalidations, windowed statistics, and application-rate alerts.
5. Kafka Connect JDBC sinks upsert selected aggregates into a separate analytics PostgreSQL database.
6. Bounded Bronze/Silver/Gold jobs preserve raw events, validate data, and build dimensions and facts.
7. Streamlit displays matching snapshots, operational metrics, test receipts, and service health.

The live matching engine and bounded medallion jobs are separate processing paths. The dashboard is an observability layer, not a write interface to Kafka or Delta.

## Technology stack

| Area | Technologies | Responsibility |
|---|---|---|
| Language | Python | Generator, integrations, helpers, dashboard |
| Source / serving DB | PostgreSQL 17 | Recruitment records and analytics sinks |
| CDC | Debezium 2.7.3 | Capture committed database changes |
| Messaging | Apache Kafka 3.8.0 (KRaft) | Durable event transport |
| Streaming | PySpark, Spark Structured Streaming | Enrichment and match scoring |
| Lake | Delta Lake | Match outputs and medallion tables |
| Python stream processing | Quix Streams 3.14 | Stateful event deduplication |
| Stream SQL | ksqlDB 7.6.1 | Windows, invalidations, alerts |
| Serialization | Avro, Schema Registry | Schema-backed event contracts |
| Integration / UI | Kafka Connect 3.7, JDBC sinks, Streamlit | Analytics serving and dashboard |
| Local environment | Docker Compose, Windows | Local multi-service development |

## Data flow and Kafka topics

### End-to-end flow

The generator writes to PostgreSQL only; it does not publish directly to Kafka. Debezium publishes source-table changes, and separate consumers process the topics. The documented default synthetic workload is 1,000,000 operations; start with a smaller workload while validating a local setup.

- **CDC and matching:** `candidate_events`, `job_events`, and `application_events` flow from Debezium to the Spark match engine.
- **Deduplication:** `application_submitted_events` enters Quix Streams; unique events are emitted to `application_submitted_dedup`.
- **Job state:** `job_state_events` and `application_state_events` feed ksqlDB projections.
- **Invalidation and analytics:** `match_invalidation_events`, `job_metrics_5m`, and `application_alerts` carry derived events and aggregates.
- **Priority and failures:** `high_priority_matches` carries scores ≥ 80; `dlq.*` topics capture duplicate, malformed, projection, and sink errors.

### Kafka integration details

Quix Streams provides stateful event deduplication. Avro and Schema Registry define the event contract with documented **BACKWARD** compatibility. Raw CDC topics remain JSON for native Spark consumption; Avro is used at the deduplication boundary.

ksqlDB calculates **5-minute tumbling** and **10-minute hopping** recruiter/job metric windows. It also generates job-state invalidations for closed, filled, or deleted jobs and application-rate alerts. Kafka Connect JDBC sinks upsert selected aggregates and alerts into analytics PostgreSQL.

Project-specific integration notes include binary ksqlDB windowed keys, epoch-millisecond window values in the documented sink path, field mappings for PostgreSQL-friendly columns, and retaining CDC delete `before` payloads. These describe this implementation and are not universal Kafka requirements.

## Streaming matching and scoring

The Spark engine maintains candidate/job state in Delta tables and scores applications after enrichment.

### Workflow

1. Read candidate, job, and application CDC topics.
2. Deduplicate candidate/job changes by key and version; upsert dimensions with Delta `MERGE`.
3. Combine new applications with pending applications from previous batches.
4. Enrich applications with candidate/job attributes and score resolvable records.
5. Write match results, retry unresolved applications, and dead-letter persistent failures.
6. Publish matches scoring at least 80 to `high_priority_matches`.

### Weighted match score

| Component | Weight | What it measures |
|---|---:|---|
| Skills overlap | 30% | Jaccard-style overlap between candidate skills and job requirements |
| Experience | 25% | Compatibility with experience requirements |
| Location | 20% | Candidate/job location and work-mode compatibility |
| Salary | 15% | Compatibility between salary expectations and job range |
| Language | 10% | Language compatibility |

| Priority | Threshold |
|---|---:|
| High | Score ≥ 80 |
| Medium | Score ≥ 65 and < 80 |
| Low | Score < 65 |

> Ensure these thresholds match the implementation you intend to publish. Earlier project notes used a different high-priority threshold, so keep the README, code, tests, and screenshots consistent.

### Matching outputs

- **Match results:** candidate/job pairs with a score and component-level explanation.
- **Pending applications:** records that cannot yet be resolved because candidate/job state is unavailable.
- **High-priority matches:** match events that meet the configured threshold.
- **Dead-letter records:** persistent failures with reason codes for inspection and recovery.

The matching score is a deterministic rules-based score, not a machine-learning prediction. It should be described as ranking or compatibility scoring rather than a trained recommendation model.

## Reliability and dead-letter handling

The pipeline includes mechanisms to avoid duplicate processing, recover unresolved records, and make failures visible.

### Reliability techniques

- **CDC:** reads committed database changes rather than relying on repeated full-table extracts.
- **Idempotent upserts:** Delta `MERGE` operations update keyed candidate/job state.
- **Checkpointing:** Spark checkpoints maintain streaming progress across restarts.
- **Retries:** unresolved application records can be retried for a bounded number of attempts.
- **Dead-letter queue (DLQ):** persistent failures are written with reason information instead of silently disappearing.
- **Deduplication:** repeated submission events are filtered using event identity/state.
- **Schema validation:** Avro schemas and Schema Registry compatibility rules help keep event contracts consistent.
- **Acceptance tests:** tests check event accounting and selected downstream behaviors.

### Deduplication accounting example

A documented acceptance test used 116 input events:

| Outcome | Count |
|---|---:|
| Unique events forwarded | 60 |
| Duplicate events sent to DLQ | 44 |
| Malformed events sent to DLQ | 12 |
| Total input | 116 |

This is a test result, not a guarantee for every workload. Keep the numbers associated with the corresponding acceptance-test receipt so that different runs are not mixed.

### Unresolved applications

Applications that cannot be matched immediately are retained for later processing. Persistent unresolved records are dead-lettered with reasons. This makes the failure visible, but a DLQ alone does not automatically repair the record: replay or correction procedures are still needed.

## Bronze–Silver–Gold pipeline

The bounded medallion pipeline provides a separate analytics path from the real-time matching engine.

| Layer | Purpose | Typical responsibilities |
|---|---|---|
| Bronze | Preserve source data | Append raw CDC/event records and source metadata |
| Silver | Clean and validate | Normalize fields, deduplicate records, apply data-quality rules |
| Gold | Analytics-ready model | Build dimensions and facts for reporting and analysis |

### Bronze

Bronze is intended to retain raw records with source context so that downstream transformations can be rerun. It should preserve enough event metadata to trace records back to their source and offsets.

### Silver

Silver applies schema and quality checks, standardizes data types and fields, and removes repeated records where the implementation's deduplication keys permit it. Invalid rows should be rejected or quarantined with an explainable reason rather than silently discarded.

### Gold

Gold organizes transformed data into reporting-friendly dimensions and facts. The exact model should be documented against the current implementation and SQL, especially the keys used to identify unique events and the strategy for handling replays.

> **Implementation caveat:** The current notes identify potential replay/idempotency gaps in some bounded writes. Do not claim that every Bronze, Silver, and Gold operation is fully idempotent until the relevant write paths and tests verify it.

## Performance engineering and results

The project records processing metrics to make bottlenecks and changes observable. Report measured results separately from targets and ensure each metric refers to a specific run and workload.

### Recorded performance examples

| Metric | Recorded value | Interpretation |
|---|---:|---|
| Initial batch duration | 73 seconds | Baseline recorded for the compared workload |
| Optimized batch duration | 41 seconds | Recorded after optimization |
| Duration reduction | About 44% | Based on 73s → 41s |
| Relative throughput | About 1.7× | Reported comparison; benchmark conditions should be stated |
| Files read per batch | 16–19 → 2–3 | Recorded reduction in files read |
| Producer-to-dedup p95 latency | 5.996 seconds | Recorded test result against a 10-second target |
| Input rate | About 501 rows/sec | Recorded workload rate |
| Observability metrics | 25 | Metrics exposed/recorded by the project |

These numbers describe specific test runs, not a universal capacity guarantee. Include workload size, warm-up conditions, machine resources, and measurement method when presenting formal benchmarks.

### Optimization focus

- Reduce unnecessary reads and repeated dimension processing.
- Inspect the cost of pending-application persistence and resolution checks.
- Track Spark micro-batch duration and input rate separately.
- Measure p95 latency against the 10-second freshness target.
- Monitor dead-letter volume and unresolved applications.
- Use repeatable workloads when comparing changes.

A performance claim is strongest when the baseline and optimized versions use the same input, environment, and measurement window.

## Streamlit dashboard

The Streamlit dashboard provides a visual view of the pipeline without acting as the main processing engine.

### Dashboard pages

| Page | What it shows |
|---|---|
| Executive Overview | Pipeline KPIs, processing paths, component health |
| Candidate–Job Matching Explorer | Ranked candidates, match scores, component breakdowns |
| Kafka Streaming & Reliability | Topic activity, consumer lag, dead-letter signals, sink freshness where available |
| Batch Medallion & Quality | Bronze/Silver/Gold output status and quality indicators |
| Engine Performance | Batch duration, throughput, row counts, stage timing |
| Testing & Health | Service probes, connector/ksqlDB status, saved test receipts |

### Cache TTLs and freshness

| Source | Cache TTL | Qualification |
|---|---:|---|
| Batch metrics JSONL | 20 seconds | Page must rerun to fetch new data after expiry |
| Kafka offsets / consumer lag | 30 seconds | TTL is not an automatic polling interval |
| Docker / Connect / ksqlDB probes | 30 seconds | Re-probed on rerun after expiry |
| PostgreSQL analytics | 30 seconds | Queried again on rerun after expiry |
| Matching CSV exports | 120 seconds | Expiry does not regenerate CSVs |
| Export freshness metadata | 120 seconds | Rechecked after expiry and rerun |
| Acceptance receipts | 300 seconds | Saved results, not a live test feed |

**Important:** The inspected code does not implement an automatic timed page rerun. Cache TTL is a reuse period, not a guaranteed page-refresh interval; a page can stay unchanged while idle after expiry.

### Matching export behavior

Matching panels read CSV snapshots exported from Delta. The **Re-snapshot Delta exports** button runs `scripts/export_delta_for_bi.py` and clears related caches. New Delta matches appear after the export script runs and the page reruns. For less manual refresh, schedule exports or query a suitable serving layer directly.

Ordinary dashboard rendering queries data and probes services; it does not write to Kafka topics, checkpoints, or Delta tables. The export button is the write-capable exception because it invokes the existing export script. Acceptance receipts are historical outcomes, not continuously running tests.

### Recommended charts for the dashboard / README

| Chart | Suggested content | Value |
|---|---|---|
| KPI cards | Batch duration, rows/sec, p95 latency vs 10s SLA, unresolved/DLQ rows, high-priority matches | One-glance pipeline status; include run timestamps |
| Batch duration | Line chart by batch ID; mark initial replay separately | Shows performance trend |
| Input throughput | Separate line chart of rows/sec by batch | Avoids mixing units on one axis |
| Application outcomes | Incoming, resolved/scored, unresolved, dead-letter by batch | Shows application flow; verify categories before stacking |
| Stage costs | Bars for pending write, resolved check, scoring write, priority check | Makes bottlenecks obvious |
| Matching explorer | Ranked candidates and five component scores | Explains why candidates rank highly |
| Dedup / DLQ | Unique forwarded, duplicates, malformed; label test/run | Demonstrates event accounting |
| Kafka lag | Lag by group/topic; trend only with saved time-series samples | Separates snapshot from historical trend |
| Data quality | Valid/quarantined rows by entity and rejection reason | Shows quality handling when lake output exists |

For the 116-event example, show 60 unique, 44 duplicates, and 12 malformed events with zero unaccounted. Do not combine figures from different runs.

Save the six screenshots using the exact `docs/images/streamlit/` paths in the gallery at the top of this README. Capture the actual running Streamlit pages rather than creating illustrative screenshots, crop out desktop clutter, and remove credentials or private connection details. GitHub renders the images as static previews; link to a deployed Streamlit app if readers should interact with the filters and tables. A short GIF can demonstrate a workflow, but embedded charts remain static.

## Acceptance tests and local setup

Run the complete acceptance suite from the repository root:

```powershell
powershell -ExecutionPolicy Bypass -File tests\run_all.ps1
