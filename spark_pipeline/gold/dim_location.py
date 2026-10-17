"""
Gold — dim_location (Type 1, sequential keys).
=================================================
Conformed city+country dimension shared by dim_job (job.city/country) and
dim_candidate (candidate.location, a free-text "City, Country" string).

Column provenance -- be honest about what's real vs. a starter seed:
  - location_key, city_norm, country      : derived directly from silver data
  - country_code, continent, timezone     : joined from reference/country_reference.csv
                                             (real, stable ISO/IANA facts, hand-maintained)
  - state_region, latitude, longitude,
    city-level timezone override           : joined from reference/city_geo_seed.csv,
                                             a STARTER set covering ~50 major/well-known
                                             cities only. Cities not in the seed get
                                             NULL here, not a guessed value.
  - postal_code                            : intentionally always NULL. A postal/ZIP
                                             code is a property of an address, not of a
                                             whole city, so there's no single correct
                                             value to put at this grain -- populate it
                                             only if you later join in address-level data.

To get full coverage instead of the starter seed, replace
reference/city_geo_seed.csv with an export from a real geocoding dataset
(e.g. GeoNames' cities1000.txt) using the same (city_norm, country) key.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import (
    SILVER_PATHS, GOLD_DIM_LOCATION, COUNTRY_REFERENCE_CSV, CITY_GEO_SEED_CSV,
)
from gold.surrogate_keys import next_start_number, assign_new_keys


def normalize_country_col(country_col):
    """Shared with silver/normalization.py's alias logic; used here and by fact_application.py."""
    from silver.normalization import COUNTRY_ALIASES
    alias_map = F.create_map([F.lit(x) for pair in COUNTRY_ALIASES.items() for x in pair])
    lowered = F.trim(F.lower(country_col))
    return F.initcap(F.coalesce(alias_map[lowered], lowered))


def _distinct_locations(spark: SparkSession):
    job = spark.read.format("delta").load(SILVER_PATHS["job"])
    from_jobs = job.select(
        F.trim(F.col("city")).alias("city"),
        normalize_country_col(F.col("country")).alias("country"),
    )

    candidate = spark.read.format("delta").load(SILVER_PATHS["candidate"])
    # candidate.location is free text "City, Country" -- split on the comma.
    parts = F.split(F.col("location"), ",")
    from_candidates = candidate.select(
        F.trim(parts.getItem(0)).alias("city"),
        normalize_country_col(F.trim(parts.getItem(1))).alias("country"),
    )

    return (
        from_jobs.unionByName(from_candidates)
        .filter(F.col("city").isNotNull() & (F.length("city") > 0))
        .filter(F.col("country").isNotNull() & (F.length("country") > 0))
        .withColumn("city_norm", F.lower(F.col("city")))
        .distinct()
    )


def _enrich(spark: SparkSession, locations):
    country_ref = spark.read.option("header", "true").csv(COUNTRY_REFERENCE_CSV)
    city_ref = (
        spark.read.option("header", "true").csv(CITY_GEO_SEED_CSV)
        .withColumnRenamed("latitude", "_seed_lat")
        .withColumnRenamed("longitude", "_seed_lon")
        .withColumnRenamed("city_timezone", "_seed_tz")
        .withColumnRenamed("state_region", "_seed_region")
    )

    enriched = (
        locations
        .join(country_ref, locations["country"] == country_ref["country_name"], "left")
        .drop("country_name")
        .join(city_ref, ["city_norm", "country"], "left")
        .withColumn("state_region", F.coalesce(F.col("_seed_region"), F.lit("Unknown")))
        .withColumn("latitude", F.col("_seed_lat").cast("double"))
        .withColumn("longitude", F.col("_seed_lon").cast("double"))
        # city-level timezone (more precise) wins over the country-level default when we have it
        .withColumn("timezone", F.coalesce(F.col("_seed_tz"), F.col("timezone")))
        .withColumn("postal_code", F.lit(None).cast("string"))  # see module docstring
        .drop("_seed_lat", "_seed_lon", "_seed_tz", "_seed_region")
    )
    return enriched


ATTRIBUTE_COLS = [
    "city", "city_norm", "country", "country_code", "continent", "timezone",
    "state_region", "latitude", "longitude", "postal_code",
]


def build_dim_location(spark: SparkSession):
    from delta.tables import DeltaTable

    locations = _distinct_locations(spark)
    if locations.isEmpty():
        return
    enriched = _enrich(spark, locations)

    natural_key = ["city_norm", "country"]

    if DeltaTable.isDeltaTable(spark, GOLD_DIM_LOCATION):
        existing_keys = (
            spark.read.format("delta").load(GOLD_DIM_LOCATION)
            .select(*natural_key, "location_key")
        )
        with_keys = enriched.join(existing_keys, natural_key, "left")
        already_keyed = with_keys.filter(F.col("location_key").isNotNull())
        brand_new = with_keys.filter(F.col("location_key").isNull()).drop("location_key")

        start = next_start_number(spark, GOLD_DIM_LOCATION, "location_key")
        newly_keyed = assign_new_keys(
            brand_new, "location_key", "LOC", start, order_by="city_norm",
        )
        staged = already_keyed.unionByName(newly_keyed).select(
            "location_key", *ATTRIBUTE_COLS
        )

        dim = DeltaTable.forPath(spark, GOLD_DIM_LOCATION)
        (
            dim.alias("d")
            .merge(staged.alias("s"), "d.location_key = s.location_key")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        staged = assign_new_keys(enriched, "location_key", "LOC", 1, order_by="city_norm")
        staged.select("location_key", *ATTRIBUTE_COLS).write.format("delta").mode("overwrite").save(GOLD_DIM_LOCATION)
