# Recruitment Platform — Batch Medallion Data Model

## Pipeline
```
Postgres --(WAL)--> Debezium --> Kafka (candidate_events, job_events, application_events)
                                        │
                                        ▼  bounded batch read (offsets bookmarked, idempotent)
                                     BRONZE   raw Debezium envelope, verbatim, append-only
                                        │
                                        ▼  flatten + dedupe + gap-check + normalize + validate
                                     SILVER   one clean row per (entity_id, version, op); rejects -> quarantine
                                        │
                                        ▼  SCD2 merge (dims) / append (fact)
                                      GOLD    dim_candidate, dim_job, fact_application, dim_date
                                        │
                                        ▼
                                 Data Warehouse (star schema, BI-ready)
```
Each stage is a separate table/Airflow task, so a bug in silver doesn't force
re-reading Kafka, and a bug in gold doesn't force re-running silver.

## Why batch reads a "streaming" source
Kafka topics are read with **bounded** `startingOffsets`/`endingOffsets`
(`common/config.OFFSET_BOOKMARK_TABLE` tracks the last offset consumed per
partition), not `readStream`. That's what makes this a *batch* job you can
schedule in Airflow, while staying safely incremental and idempotent — a
rerun with the same bookmark reads nothing new.

The SCD2 logic in `gold/scd2.py` is written as "apply this batch of already
-deduped CDC rows to the dimension," with no batch-specific assumptions. If
this later moves to Spark Structured Streaming, the same `apply_scd2()`
function is called from a `foreachBatch` — only the trigger changes.

## Bronze
- **Grain**: one row per Kafka message (per `_kafka_topic`, `_kafka_partition`, `_kafka_offset`).
- **Contract**: no parsing of `before`/`after`, no dropped fields, no type coercion beyond the declared row schema. This is the replay source of truth.
- **Dedup key**: `_event_id = sha2(topic|partition|offset)` — a Kafka offset is unique forever, unlike `entity_id+version` which can legitimately repeat after a reprocess.
- Partitioned by `_ingest_date`, `op`.

## Silver
- **Grain**: one row per `(entity_id, version, op)` — a cleaned, deduplicated, flattened CDC event. Still history, not yet collapsed into a dimension.
- **Flatten**: `op in (c,u,r)` → use `after`; `op == 'd'` → use `before` (Debezium nulls `after` on delete) and stamp `is_deleted=true`.
- **Dedup**: window over `(entity_id, version, op)` — protects against a Kafka redelivery producing two bronze rows for the same logical change.
- **CDC gap detection**: `has_version_gap=true` when an entity's `version` jumps by more than 1 vs. the previous known version for that entity — flags a possibly-missed WAL event without dropping the (still latest-known) row.
- **Normalize** (`silver/normalization.py`): skills/certifications → lowercased + alias-mapped array; country → alias-mapped + title-cased; salary strings (`"$120,000"`, `"120K"`) → integer; experience strings (`"2-4 years"`, `"2 to 4 years"`) → `experience_min_years`/`experience_max_years`.
- **Validate** (`silver/validation.py`): named rules per entity (salary ≥ 0, GPA 0–4, deadline ≥ post_date, status/work_mode enums, required FKs, etc.). Rows failing any rule are **not dropped silently** — each failure is written to `quarantine/<entity>` as `(record_id, entity, validation_rule, rejection_reason, processed_at)`, and the row is excluded from silver.

## Gold — star schema

| Table | Type | Grain | Notes |
|---|---|---|---|
| `dim_candidate` | SCD2 | one row per candidate per distinct attribute state | closed on `valid_to`, `is_current` flag |
| `dim_job` | SCD2 | one row per job post per distinct attribute state | same pattern |
| `dim_company` | Type 1 | one row per distinct company name | conformed across job postings + candidate employment history |
| `dim_location` | Type 1 | one row per distinct (city, normalized country) | conformed across job postings + candidate free-text location |
| `dim_date` | Type 1 (static) | one row per calendar day | standard BI calendar table |
| `fact_application` | Append-only lifecycle fact | one row per application **event** (applied → viewed → interview → offer → rejected/accepted) | **not** SCD2 — see below |

### Why `dim_company` and `dim_location` are Type 1, not SCD2
Unlike a candidate's title or a job's salary, a company's name or a
location's identity isn't something the warehouse needs to time-travel
through — there's nothing meaningful about "as of last month, this company
was called something else." They exist purely so `fact_application` (and
potentially future facts) can join to one stable key instead of repeating
raw text, and so BI tools get a clean list of distinct companies/locations
to filter and group by. New values are inserted; nothing is ever closed
out or versioned.

Both are sourced from **both** silver tables that mention them — company
from `job_posts.company` *and* `candidate_info.current_company` /
`previous_companies[]`; location from `job_posts.city/country` *and*
candidate's free-text `location` field (split + the same country-alias
normalization used in silver) — so the same real-world company or place
gets exactly one row no matter which side of the data it came from.

### Enriched columns — what's real vs. a starter seed
`dim_location` and `dim_company` carry extra reference columns that don't
exist anywhere in the source Postgres tables (lat/long, state/region,
company size/revenue/founding year, etc.). Rather than fabricate
plausible-looking values, each column is honestly one of three things:

| Kind | Examples | Source |
|---|---|---|
| Derived from real data we already have | `dim_company.industry` | most common `job_posts.industry` for that company |
| Real, stable, hand-maintained facts | `country_code`, `continent`, country-level `timezone` | `reference/country_reference.csv` — real ISO/IANA data, not from an API |
| Starter seed, partial coverage | `state_region`, `latitude`, `longitude`, city-level timezone override, `company_size`/`company_type`/`founded_year`/`headquarters_country`/`website_domain`/`employee_count`/`revenue_range` | `reference/city_geo_seed.csv` (~50 major cities) and `reference/company_reference_seed.csv` (a handful of well-known companies) |

Anything not covered by the starter seeds shows `"Unknown"`/`NULL`,
never a guessed value. `dim_company.is_enriched` flags which rows actually
matched the firmographic seed. `dim_location.postal_code` is always
`NULL` — a postal code belongs to an address, not a whole city, so there's
no single correct value at this grain.

**To get real coverage in production**, replace the two seed CSVs with an
export from an actual geocoding dataset (e.g. GeoNames) and a firmographic
provider (Clearbit, Crunchbase, LinkedIn), keyed the same way
(`city_norm`+`country` / `company_name`) — no pipeline code changes needed.

### Sequential surrogate keys
`dim_company` and `dim_location` use human-readable sequential keys
(`COMP_000047`, `LOC_000125`) via `gold/surrogate_keys.py`, instead of a
hash. Existing rows keep their key forever across reruns; only genuinely
new companies/locations get the next number(s). Because these keys aren't
derivable from the data itself, `fact_application` resolves them with a
real join to `dim_company`/`dim_location` (via `dim_job`'s as-of
company/city/country), rather than recomputing them.

### Why `fact_application` is a fact, not a dimension
Applications are a **process with a lifecycle**, not a slowly-changing entity
description — the interesting analytics (funnel conversion, time-in-stage,
drop-off rate) require every stage transition as its own row, not a
"current status" snapshot. Collapsing it into an SCD2 dimension would erase
exactly the history that makes it useful.

### As-of join, not "current" join
Each fact row links to `dim_candidate`/`dim_job` via the row that was
`is_current` **as of the event's timestamp**, using `(natural_key, valid_from)`
as the composite key — not a `candidate_id` join to today's current row.
Otherwise a fact from 3 months ago would silently show today's job title.

## Data quality feedback loop
`quarantine/<entity>` isn't a dead-letter dump — it's meant to be reviewed
periodically to (a) tighten `SKILL_ALIASES`/`COUNTRY_ALIASES` as new
variants surface, and (b) catch genuine upstream bugs (e.g. Postgres app
code writing an invalid `work_mode`) before they reach the warehouse.
