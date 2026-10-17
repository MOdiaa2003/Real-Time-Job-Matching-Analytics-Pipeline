"""
Shared configuration for the batch medallion pipeline.
=======================================================
One place for paths, Kafka connection info, and the topic <-> entity map.
Everything here is overridable via env vars so the same code runs in
dev / CI / prod without edits.
"""

import os

# ── Kafka (source: Debezium CDC topics) ──────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# entity -> kafka topic (matches TOPIC_NAME_OVERRIDES in debezuim_connector.py)
ENTITY_TOPICS = {
    "candidate": "candidate_events",
    "job": "job_events",
    "application": "application_events",
}

# ── Lakehouse storage root ────────────────────────────────────────────────
# Swap for s3a://... or abfss://... in prod; layout stays identical.
LAKE_ROOT = os.getenv("LAKE_ROOT", "/home/claude/lake")

BRONZE_ROOT = f"{LAKE_ROOT}/bronze"
SILVER_ROOT = f"{LAKE_ROOT}/silver"
GOLD_ROOT = f"{LAKE_ROOT}/gold"
QUARANTINE_ROOT = f"{LAKE_ROOT}/quarantine"
CONTROL_ROOT = f"{LAKE_ROOT}/_control"  # offset bookmarks, run metadata

BRONZE_PATHS = {e: f"{BRONZE_ROOT}/{e}" for e in ENTITY_TOPICS}
SILVER_PATHS = {e: f"{SILVER_ROOT}/{e}" for e in ENTITY_TOPICS}
QUARANTINE_PATHS = {e: f"{QUARANTINE_ROOT}/{e}" for e in ENTITY_TOPICS}

GOLD_DIM_CANDIDATE = f"{GOLD_ROOT}/dim_candidate"
GOLD_DIM_JOB = f"{GOLD_ROOT}/dim_job"
GOLD_FACT_APPLICATION = f"{GOLD_ROOT}/fact_application"
GOLD_DIM_DATE = f"{GOLD_ROOT}/dim_date"
GOLD_DIM_COMPANY = f"{GOLD_ROOT}/dim_company"
GOLD_DIM_LOCATION = f"{GOLD_ROOT}/dim_location"

# ── Static reference/enrichment seed files ────────────────────────────────
# These ship with the repo as plain CSVs (not fetched from any API). They
# cover real, stable facts (ISO country codes, continents, IANA timezones)
# and a small STARTER set of well-known cities/companies for the fields
# that genuinely require an external data provider (lat/long, state/region,
# company firmographics). Anything not in these seeds shows up as
# "Unknown"/null in the dimension -- swap REFERENCE_ROOT for a real
# geocoding/firmographic export (e.g. GeoNames, Clearbit, Crunchbase) to
# get full coverage in production.
REFERENCE_ROOT = os.getenv("REFERENCE_ROOT", os.path.join(os.path.dirname(os.path.dirname(__file__)), "reference"))
COUNTRY_REFERENCE_CSV = os.path.join(REFERENCE_ROOT, "country_reference.csv")
CITY_GEO_SEED_CSV = os.path.join(REFERENCE_ROOT, "city_geo_seed.csv")
COMPANY_REFERENCE_SEED_CSV = os.path.join(REFERENCE_ROOT, "company_reference_seed.csv")

OFFSET_BOOKMARK_TABLE = f"{CONTROL_ROOT}/kafka_offsets"  # legacy name, unused (bookmarks are now JSON files directly under CONTROL_ROOT)

# ── Batch sizing ───────────────────────────────────────────────────────────
# Bronze is read in bounded batches (startingOffsets/endingOffsets), not
# continuous streaming, per the "batch, medallion" requirement. Re-run on a
# schedule (Airflow/cron); offsets are tracked in OFFSET_BOOKMARK_TABLE so
# each run only picks up new messages -> idempotent, incremental.
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "500000"))
