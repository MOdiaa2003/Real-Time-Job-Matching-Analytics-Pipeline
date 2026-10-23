# Power BI loading guide - every dataset this pipeline generates

Companion file to the commit history for the BI export tool
(`scripts/export_delta_for_bi.py`). All three data sources are LOCAL, so Power
BI Desktop connects directly - no gateway needed.

## Data source 1 - Kafka streaming layer results (analytics Postgres :5433)

What: exactly what the streaming layer produced - 5-min tumbling + 10-min
hopping job metrics and spam alerts.

```
Get Data -> PostgreSQL database
Server:  localhost
Database: analytics
Data Connectivity mode: Import        (DirectQuery also works; local, so either)
PostgreSQL database credential: User "postgres", password per docker-compose.yml
```
Under Database: pick `job_metrics_5m` and `application_alerts`.

Column gotcha: WINDOW_START / WINDOW_END arrive as epoch-millis (BIGINT).
Convert to real datetimes in Power Query:
`WindowStartDT = #datetime(1970,1,1,0,0,0) + #duration(0,0,0, [WINDOW_START] / 1000)`

Suggested DAX measures once loaded:
```
Total Applications = SUM(job_metrics_5m[APPLICATIONS_CNT])
Total Matches      = SUM(job_metrics_5m[MATCHES_CNT])
Match Rate %       = DIVIDE([Total Matches], [Total Applications])
Alert Rate         = DIVIDE(COUNTROWS(application_alerts), [Total Applications])
```
Nice visuals: clustered column per window (applications vs matches) with the
window datetime axis; alerts table sliced by ALERT_REASON (all values are
`apply_spam` by design).

## Data source 2 - the business entities (source Postgres :5432)

What: the live recruitment tables that ACCELERATE everything else - use them as
dimension tables so dashboards can slice by category/city/seniority.

```
Get Data -> PostgreSQL database
Server:  localhost
Database: recruitment
Tables: candidate_info, job_posts, applications
```
Model relationships (star-ish):
- applications[CANDIDATE_ID] -> candidate_info[CANDIDATE_ID]
- applications[POST_ID]      -> job_posts[POST_ID]
- job_metrics_5m[JOB_ID]     -> job_posts[POST_ID]   (1:* inactive, activatable by USERELATIONSHIP)

ARRAY-text columns in candidate_info (skills, previous_companies, ...) arrive
as one string per row; for filtering, add a Power Query custom column:
`HasSkill = if Text.Contains(SomeLowered([skills]), "<skill>", Comparer.OrdinalIgnoreCase) then 1 else 0`

## Data source 3 - batch engine outputs (Delta Lake tables)

What: the Spark engine's curated gold tables - dimensional states, pending
applications, final match results, invalidations, dead letters.

Two supported ways:

A) Prebuilt files (RECOMMENDED - simplest):
```
python scripts\\export_delta_for_bi.py        # ~1 min, offline
```
produces:
- exports\\<table>.csv            (flat single-file, arrays flattened to comma text)
- exports\\<table>.parquet        (spark folder layout, single part file)
- exports\\csv\\<table>\\         (spark's own output dir)

In Power BI: Get Data -> Text/CSV for the flat files, or Folder + Transform for
the parquet dirs. Current verified snapshot sizes:
match_results 23 499 rows / 29.6 MB, job_dim 10 147, candidate_dim 7 181,
match_invalidations 255, dead_letter_applications 83, pending_applications 41.

B) Native Delta connector (Power BI Desktop / Fabric):
Get Data -> More... -> Delta Lake, point it at
`<repo>\\streaming_lake\\<table>` and load. Works too but the prebuilt CSVs
are simpler for interviews.

Relationship note: match_results rows do NOT carry a separate id that
joins 1:1 to the analytics aggregates - join by POST_ID/CANDIDATE_ID against
the_EXPORT source-DB dims from data source 2.

## Data source 4 - Kafka topics themselves (invalidations, dedup stream, DLQ)

Kafka is NOT a native Power Bi source. Two supported options:
1. Serving-layer route (the one the pipeline already implements): ksqlDB CTAS ->
   `job_metrics_5m` / `application_alerts` topics -> Debezium JDBC sink ->
   analytics Postgres -> source 1 above. Everything Kafka saw ends up in
   Postgres; query THAT in Power BI. This is the production pattern too.
2. Push streaming dataset: create a Power BI Streaming dataset (Add streaming
   data) and push from python via the Power BI REST API if the user wants
   sub-minute refresh for one or two KPI numbers; heavier - only add if the
   5-minute DB refresh truly isn't enough.

## Refresh cadence

Pipeline pushes continuously; Power BI Import needs a manual/auto refresh
(File -> Options + Scheduled refresh only applies to the gateway, local .pbix
refresh happens on open or manually per Home -> Refresh). Typical demo flow:
run `scripts\\run.ps1 -WithGenerator` for 5 minutes -> press Refresh in
Power BI -> numbers move.
