# Troubleshooting guide (things that actually bit this project)

Ordered by how often they will bite you on this stack (Windows + Docker Desktop).

## 1. Connect / CSR / ksql REST calls from PowerShell

`curl.exe` under Windows PowerShell 5.1 strips embedded `"` in
`--data-binary` arguments, so this SHIPS invalid JSON to ksql:

```powershell
curl.exe -X POST -H "Content-Type: application/vnd.ksql.v1+json" --data-binary '{"ksql":"SHOW QUERIES;"}' http://localhost:8088/ksql   # WRONG
```

ksql answers `Invalid JSON ... expecting double-quote to start field name`.
Use `Invoke-RestMethod` for calls whose errors you don't need verbatim, and
keep a `curl`-based file body for the ones you do:

```powershell
Invoke-RestMethod -Uri http://localhost:8088/ksql -Method Post `
  -ContentType "application/vnd.ksql.v1+json; charset=utf-8" `
  -Body '{"ksql":"SHOW QUERIES;"}'
```

Same for `docker inspect --format "..."`: single-quoted here and never
`{{...}}` placeholders containing embedded quotes.

## 2. Connect image: missing plugins

The stock `cp-kafka-connect` has the Avro converters but NOT Debezium's
JDBC sink pieces; `docker/kafka-connect-extended.Dockerfile` layers
debezium-connector-postgres 2.7.3 + kafka-connect-jdbc 10.7.1 jars from
packages.confluent.io / repo1.maven.org. If Confluent Hub CDN times out
(it did, repeatedly, from this dev machine), pull jars from those mirrors.

## 3. JDBC sink rejects the ksql windowed records

Symptoms seen live and their fixes:

* KEY converter failure with JSON: ksql windowed keys are a proprietary
  binary blob, not JSON -> `key.converter=org.apache.kafka.connect.storage.StringConverter`.
* Unknown column names: ksqlDB does NOT repeat GROUP BY keys in windowed
  table VALUES -> every aggregate value schema carries
  `LATEST_BY_OFFSET(job_id) AS JOB_ID_FLAT` projections; the sink looks
  for the value schema alone.
* SQL case mismatch / "column WINDOW_START does not exist": Debezium
  maps identifiers verbatim -> the sink uses a `ReplaceField$Value` SMT
  renaming `JOB_ID_FLAT -> job_id`, `WINDOW_START/WINDOW_END -> window_start/window_end`
  (`RenameField` does not exist in Kafka 3.7 transforms) AND
  `quote.identifiers=false` (NOT `quote.sql.identifiers`) to stop Postgres
  case-folding.
* window_start/window_end BIGINT, not TIMESTAMP: ksql WINDOWSTART is
  BIGINT epoch ms and maps as such; `database/init_analytics.sql`
  declares those columns BIGINT for exactly this reason.

## 4. ksqlDB behaviors that cost us time

* `/ksql` endpoint + trailing `;` for DDL; `/query` is for push queries only.
* CREATE TABLE (not CREATE STREAM) for windowed aggregations; COUNT(DISTINCT)
  unsupported in this build -> COUNT(*).
* Repartitioning on a key column that is NOT the value: key must be COALESCED
  into the value via `LATEST_BY_OFFSET(...)` with a DIFFERENT alias than the
  key column name (ksql rejects duplicate value column names) - see
  `ksql/03_windowed_metrics.sql`.
* ksqlDB normalizes UNQUOTED identifiers to UPPERCASE -> JSON legs emit
  `JOB_ID`-style keys. Consumers (Spark) must map casing; documented in
  `schemas/match_invalidation.avsc`.
* Legacy schema versions: after rebuilding `job_metrics_5m` the sink
  re-created avro schema version (id bumped) - if a sink stops with
  schema-incompatible records, delete + recreate the TOPIC (data volume is
  re-materialized from ksql state), never edit the registry in place.

## 5. Spark on Windows

* Relative Delta paths are UNSAFE for `DeltaTable.createIfNotExists` -
  it intermittently treats `sub/dir` as a catalog name ("X is not a valid
  name"). The engine normalizes `CHECKPOINT_ROOT`/`DELTA_ROOT` to absolute
  repo-rooted paths (`_abs_root`).
* Reading Delta with a plain pyspark session fails `DATA_SOURCE_NOT_FOUND:
  delta` - delta-spark pip does not add jars to the classpath; configure
  `spark.jars.packages` (exactly how `build_spark()` does) in any utility
  session.
* Redirecting engine stdout to a FILE makes python block-buffer, so the
  log lags minutes behind reality ("looks stuck in ivy"). Judge progress
  by `Get-NetTCPConnection` of the java child (broker connections at 9092),
  not by tailing the file.
* First run of a session that resolves spark packages can take 2-3 min
  (ivy/maven). That is normal; later runs are cached.

## 6. Producer config errors

`enable.thread.pin` existed in older librdkafka builds; confluent-kafka
2.9 REJECTS it as unknown property - it was removed from the producer config.

## 7. PowerShell 5.1 gotchas

* BOM-less .ps1 files MUST be pure ASCII (multibyte gets mis-decoded).
* Native commands failing via stderr raise noisy `NativeCommandError`
  display lines - harmless as long as you switch on `$LASTEXITCODE`.
* `2>$null` on native commands can break output capture; prefer explicit
  object parsing.
* Windows reserved device names: a file named `con.json` exists on disk but
  is INVISIBLE to PowerShell (Test-Path lies about it) and `git add` will
  hard-fail on it. Delete with `cmd /c del /f /a \\?\C:\..\..\con.json`.
  This bit us during a `git add -A` commit.

## 8. Data-flow quick checks

```powershell
# broker producers/consumers lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --all-groups --describe

# ksql persistent queries health
Invoke-RestMethod -Uri http://localhost:8088/ksql -Method Post -ContentType "application/vnd.ksql.v1+json" -Body '{"ksql":"SHOW QUERIES;"}'

# silent data loss accounting (input = output + DLQ? per event_id)
.venv-streams\Scripts\python scripts\audit_round_trip.py

# connectors
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/jdbc-sink-job-metrics-5m/status
```

## 9. Reset hygiene

`scripts/reset.ps1` does `docker compose down -v` for THIS compose project
ONLY (containers renamed `*_old_jobposting` from the previous project are
untouched, and they should never be deleted). It also clears Spark
checkpoints + Delta lake unless you pass `-KeepSparkState`.
