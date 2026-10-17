"""
Silver layer — normalization.
==============================
recruitment_lifecycle_generator.py draws skills/certifications/locations
from a controlled vocabulary (data_used.py), so this synthetic feed is
already fairly clean. In production the same topics get written to by
other systems too, so these rules exist to make silver robust to case
variance, whitespace, aliasing, and free-text salary/experience strings
-- not just to fix the sample data as it is today.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ── Skill aliasing ─────────────────────────────────────────────────────────
# Extend this map (or load it from a governed reference table) as new
# synonyms show up in quarantine review.
SKILL_ALIASES = {
    "pyspark": "spark",
    "apache spark": "spark",
    "python programming": "python",
    "js": "javascript",
    "powerbi": "power bi",
    "tableau/powerbi": "data visualization tools (tableau/powerbi)",
    "nlp": "natural language processing (nlp)",
    "ml": "machine learning",
}

# ── Country aliasing ────────────────────────────────────────────────────────
COUNTRY_ALIASES = {
    "usa": "united states", "us": "united states", "u.s.": "united states",
    "u.s.a.": "united states", "uk": "united kingdom", "u.k.": "united kingdom",
}


def _alias_map_col(alias_dict: dict):
    return F.create_map([F.lit(x) for pair in alias_dict.items() for x in pair])


def normalize_skill_array(col_name: str):
    """lower/trim each element, then apply the alias map; keep order, drop empties."""
    alias_map = _alias_map_col(SKILL_ALIASES)
    cleaned = F.transform(
        F.col(col_name),
        lambda s: F.trim(F.lower(s)),
    )
    aliased = F.transform(
        cleaned,
        lambda s: F.coalesce(alias_map[s], s),
    )
    return F.filter(aliased, lambda s: (s.isNotNull()) & (F.length(s) > 0))


def normalize_country(col_name: str):
    alias_map = _alias_map_col(COUNTRY_ALIASES)
    lowered = F.trim(F.lower(F.col(col_name)))
    return F.initcap(F.coalesce(alias_map[lowered], lowered))


def normalize_salary(col_name: str):
    """Coerce '$120,000' / '120K' / 120000 / '120000' -> integer dollars."""
    raw = F.col(col_name).cast("string")
    digits_only = F.regexp_replace(raw, r"[^0-9.Kk]", "")
    is_k = digits_only.rlike(r"[Kk]$")
    numeric = F.regexp_replace(digits_only, r"[Kk]$", "").cast("double")
    return F.when(raw.isNull(), F.lit(None).cast("int")) \
             .when(is_k, (numeric * 1000).cast("int")) \
             .otherwise(numeric.cast("int"))


def split_experience_range(col_name: str):
    """'2-4 years' / '2 – 4 years' / '2 to 4 years' -> (min_years, max_years)."""
    raw = F.regexp_replace(F.col(col_name), r"(–|—|to)", "-")
    min_years = F.regexp_extract(raw, r"(\d+)\s*-\s*\d+", 1).cast("int")
    max_years = F.regexp_extract(raw, r"\d+\s*-\s*(\d+)", 1).cast("int")
    # single value like "5+ years" -> min=5, max=null
    single = F.regexp_extract(raw, r"^(\d+)\+?\s*years?$", 1).cast("int")
    min_out = F.coalesce(min_years, single)
    max_out = max_years
    return min_out, max_out


def add_candidate_normalization(df: DataFrame) -> DataFrame:
    min_y, max_y = split_experience_range("experience_range")
    return (
        df.withColumn("skills_norm", normalize_skill_array("skills"))
          .withColumn("certifications_norm", normalize_skill_array("certifications"))
          .withColumn("country_norm", normalize_country("location"))
          .withColumn("current_salary_norm", normalize_salary("current_salary"))
          .withColumn("experience_min_years", min_y)
          .withColumn("experience_max_years", max_y)
    )


def add_job_normalization(df: DataFrame) -> DataFrame:
    min_y, max_y = split_experience_range("years_experience")
    return (
        df.withColumn("skills_needed_norm", normalize_skill_array("skills_needed"))
          .withColumn("certifications_norm", normalize_skill_array("certifications"))
          .withColumn("country_norm", normalize_country("country"))
          .withColumn("salary_norm", normalize_salary("salary"))
          .withColumn("experience_min_years", min_y)
          .withColumn("experience_max_years", max_y)
    )
