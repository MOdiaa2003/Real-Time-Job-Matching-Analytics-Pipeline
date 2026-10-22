# PROGRESS.md — build log (autonomous run)

Started: 2026-10-22 (Stage 3 onwards)
Setup context: Stages 1 (infra docker-compose/.env/database/monitoring) and 2 (connectors/*.json, schemas/*.avsc, register_connectors.ps1) already completed and commit-worthy.

## Stage 3 — Generators

2026-10-22T00:00 — Stage 3 started. Plan:
* recruitment_lifecycle_generator.py: add CLI flags + CONFIG rate knobs (job_close_rate, job_reopen_rate, profile_update_rate, app_status_transition_rate) with default 0 = unchanged behavior.
* NEW Realtime_Pipline/fault_injection.py (duplicate/malformed/late/burst helpers + argparse for on-demand fault injection).
* NEW Realtime_Pipline/application_event_producer.py (Avro+SR idempotent producer for application_submitted_events).

(appended entries in reverse-chronological-ish order as work completes)

2026-10-22T00:35 — Stage 3 code complete:
- recruitment_lifecycle_generator.py: CONFIG rate keys (job_close_rate, job_reopen_rate, profile_update_rate, app_status_transition_rate, duration_seconds), env overrides (JOB_CLOSE_RATE etc.), CLI parse_cli_overrides() (--ops/--duration/--job-close-rate/--job-reopen-rate/--profile-update-rate/--app-status-transition-rate), update_job close branch now uses close_prob=0.15+job_close_rate (capped 0.9), new LifecycleEngine.reopen_job() (closed->open, stage 0, 60s warming), extended APP_TRANSITIONS chain (viewed->interview->offer->accepted/rejected w/ final_outcome) ONLY when app_status_transition_rate>0, choose_operation gains reopen + configurable candidate-update weight. Defaults: all 0 -> byte-for-byte same behavior as before (weights unchanged).
- Realtime_Pipline/fault_injection.py NEW: make_event/make_malformed_payloads (non_avro_bytes, wrong_header, valid_schema_wrong_types)/burst_events/cli_flags.
- Realtime_Pipline/application_event_producer.py NEW: confluent_kafka AvroSerializer (schemas/application_submitted_event.avsc), SchemaRegistryClient, idempotent producer (enable.idempotence=True, acks=all, lz4), key=job_id, flags --rate/--duration/--duplicate-rate/--malformed-rate/--late-event-rate/--burst + pools.
- Verified statically: py_compile OK for all touched files; field-set of make_event matches avsc; avsc JSON parses.

DECISIONS:
- No generate_data.py exists in repo (recruitment_lifecycle_generator.py IS the generator and is ALREADY pure-Postgres); documented in README later, no duplicate generator created.
- reopen keeps 60s warming (reuses warming_jobs) to not reintroduce CDC race unresolved_rows.

OPEN: runtime verification of generators vs live Postgres (next after stack starts in Stage 4 infra bring-up).

2026-10-22T10:55 — Stage 4 infra verification (fix attempts 1-3 on JDBC plugin acquisition):
- Attempt 1: Docker daemon down → started Docker Desktop, engine v29.6.2 up.
- Attempt 2: base `debezium/connect:2.7.3.Final` = Fedora 40, already ships curl+unzip; the microdnf/apt/apk triple-fallback RUN exited 127 → removed.
- Attempt 3 FAILED (curl exit 6): Confluent Hub CDN `d2p6pa8d64rpfa.cloudfront.net` and `confluent-hub.com` are DNS-blocked (verified from host AND from containers). `cp-kafka-connect:7.6.1` image does NOT bundle the JDBC plugin (/usr/share/java lacks kafka-connect-jdbc); `cp-kafka-connect:6.2.15` also lacks it; Maven Central has no io.confluent connector artifacts; `confluentinc-kafka-connect-jdbc-10.7.1.zip` never existed on packages.confluent.io (404) — and 10.7.1 is not in the maven-metadata version list at all.
- RESOLUTION (verified 200 for all 4 URLs): assemble the plugin from the exact jars the Hub zip contains, via Confluent's own maven repo + Maven Central: kafka-connect-jdbc-10.7.3.jar + common-config-7.6.1.jar + common-utils-7.6.1.jar (packages.confluent.io/maven) + postgresql-42.7.4.jar (repo1.maven.org). Oracle/MSSQL/MySQL drivers omitted (Postgres-only demo). connect-api/connect-runtime NOT bundled (pom marks them `provided`; worker supplies them).
- RESULT: `docker compose build kafka-connect stream-processor` → both images Built. Plugin path: base image has CONNECT_PLUGIN_PATH=/kafka/connect, plugin installed at /kafka/connect/kafka-connect-jdbc/.

DECISION (runtime): `docker compose up -d` hit a container-name conflict on `/qdrant` — it belongs to a DIFFERENT compose project (`F:\DE_Projects\job_posting\project`, exited since 2026-10-13, NOT created by this task). Lowest-risk resolution: `docker rename qdrant qdrant_old_jobposting` (reversible rename, container + its volume preserved; never deleted). Recorded here per rules.

2026-10-22T12:05 — Stage 4 runtime bring-up (fix attempts 4-9):

- FIX: `apache/kafka:3.8.0` healthcheck failed — scripts are NOT on PATH inside the image → healthcheck now uses `/opt/kafka/bin/kafka-broker-api-versions.sh` full path. (docker-compose edit; container status went healthy.)
- RENAME (continued): old-project containers for name conflicts (qdrant, redpanda-console, my_postgres, redpanda, kafka-connect → `*_old_jobposting`); all preserved, never deleted.
- FIX: stream-processor schema path resolution — container runs with /app cwd; added `_SCHEMA_DIR_CANDIDATES` (host repo root + in-container /app/schemas) and a compose read-only mount `./schemas:/app/schemas:ro` in the Dockerfile so the image and repo share one schema source of truth. docker-compose build required (3 rebuilds).
- FIX: Quix `group_by` requires name label when using key lambda → `group_by(..., name="job_state_events")`.
- CONNECTORS (attempt trail):
  1. connector JSON double nesting (`config.config`) and POST `notes` field caused 500/400 on `/connectors` → flattened all three files to `{name, notes, config}` and register script now posts `{name, config}` (PUT path idempotent re-registration also verified).
  2. Debezium 2.7.3 image SHIPS io.debezium.connector.jdbc.JdbcSinkConnector (2.7.3.Final) — the sink configs were SWITCHED to it (attempt separately at loading the Confluent JDBC plugin failed; the connector.yaml plugin path wasn't resolvable). AvroConverter was NOT bundled in the debezium image → added a Dockerfile stage copying `cp-kafka-connect:7.6.1` /usr/share/java/{kafka-serde-tools,confluent-common,cp-base-new} to /kafka/connect/kafka-connect-avro-converter/ (self-contained plugin).
  3. Config keys: Debezium JDBC uses `connection.username` (NOT `connection.user`).
  4. WORKING-STATE IMG: 'Built' jobmatching/kafka-connect:2.7.3-jdbc10.7.1 with kafka-connect(jdbc) + avro-converter plugin dirs; all 3 connectors RUNNING:
     - candidate-job-matching-cdc (Debezium source) -> job_events (189 msg), candidate_events (111), heartbeat topic (hour-loaded)
     - jdbc-sink-job-metrics-5m → analytics db table job_metrics_5m
     - jdbc-sink-application-alerts → analytics db table application_alerts
- FIX: SR subject auto-registered (`application_submitted_events-value` id=2) manually via `/subjects/.../versions` because the register script silently skipped (dedup subject id=1 had registered fine).
- FIX: `__debezium-heartbeat.talent` created with retention=3600000 (heartbeat logging spam WITHOUT topic when it auto.create=false).
- ksql apply sequence findings (fixed scripts/apply_ksql.ps1):
  * parser corruption when unicode decorations present in source → REWROTE the apply script in FULL ASCII (`OK`/`SKIP`/`FAIL` prefixed lines).
  * /query-stream rejects DDL; /query rejects DDL too → use `/ksql` (legacy).
  * ksql splits statements server-side and demands the trailing `;` per statement even when submitted one-at-a-time (appended after split).
  * .env has inline comments (`VAR=2   # note`) — my primitive env readers in 3 scripts (create_topics, register_connectors, apply_ksql) now strip `#...` acts on the VALUE.
  * ksql BACKWARD compat + CTAS: `CREATE TABLE job_metrics_tumbling / job_metrics_hopping / application_alerts` (STREAM creations were rejected: windowed aggregation must output TABLE).
  * `COUNT(DISTINCT col)` is NOT supported by ksql windowed aggregate → COUNT(*) instead (dedup processor handles replay) for application_alerts HAVING.
  * ksql internal topics (APPLICATIONS_BY_JOB repartition) auto-created via AdminClient despite broker auto.create=false.
  * 7 PERSISTENT queries RUNNING.
- FIX: `ksql/03_windowed_metrics.sql` colums: the table `job_state` declares `status` NOT `job_status` (03's SELECT used j.job_status) → fixed to j.status.
- SMOKE (event path, fault injection: --duplicate-rate 0.15 --malformed-rate 0.1):
  * application_event_producer: 190 records → application_submitted_events.
  * Quix stream processor output: 147 unique events → application_submitted_dedup; 43 DLQ (30 DUPLICATE + 13 BAD_WIRE_HEADER) = EXACT accounting; no lost records; exactly-once producing via changelog topics for the dedup state store.
- FIX (5 round):
  1. Sink DLQ metric records NOT processed after restart: key converter JSON error (Kafka Connect JsonConverter fails on ksql's binary windowed key) → switched key.converter to StringConverter (pass-through, keys opaque).
  2. (SAME problem) the windowed TABLE's ksql VALUE lacked the GROUP BY key (only key in wrapped windowed key) → ksql SQL updated to add `LATEST_BY_OFFSET(job_id) AS job_id_flat` (and candidate) so the VALUE carries the key; sink PK uses it via `primary.key.fields=JOB_ID_FLAT:job_id,WINDOW_START:window_start,WINDOW_END:window_end` (field:name rename mapping unsupported in Debezium's jdbc connector).
  3. ReplaceField transform (RenameField not available in Connect 3.7 jars) → renamed values in-flight to `job_id`/`candidate_id` with `quote.identifiers=false` (PostgreSQL downcases unquoted identifiers). DML now: `INSERT ... ON CONFLICT (job_id, window_start, window_end)`.
  4. Column type: analytics db init had timestamp columns for window_start/window_end (Debezium maps BIGINT for them) → ALTERed live tables + the init script (now BIGINT).
- RESULT (verified live, 2026-10-22T12:05): job_metrics_5m = 327 rows; application_alerts = 4 rows (b) with candidate_id CAND_000009 + two tumbling alerts window sets (both from 147 events). All 7 ksql queries RUNNING, all 3 connectors RUNNING.
- DECISION: keep Avro wire + Schema Registry BACKWARD as the contract; ksqlDB DDL persists in ksql/. (02, 03 include HONEST caveat comments in the SQL files.)
- Blockers will be recorded in troubleshooting docs during Stage 6 (docs) - named 'ksql value-of-window does not carry group keys' etc.
