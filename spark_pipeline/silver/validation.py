"""
Silver layer — validation & quarantine.
========================================
Invalid records never silently disappear: each row is checked against a
named rule; failures are written to a quarantine table with the reason,
and pass everything else through. One raw row can trigger multiple rule
failures -- each becomes its own quarantine record (record_id, entity,
validation_rule, rejection_reason, processed_at), so nothing is lost and
a data steward can see exactly why a row was dropped.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.config import QUARANTINE_PATHS

# Rule builders are FUNCTIONS, not module-level constants. F.col(...) needs
# an active SparkSession/SparkContext under the hood; building these at
# import time (before spark-submit has created one) raises an
# AssertionError. Each builder is called lazily, inside validate_and_split,
# after Spark is already running.

def _candidate_rules():
    return [
        ("candidate_id_not_null", F.col("candidate_id").isNotNull()),
        ("salary_non_negative", F.col("current_salary_norm").isNull() | (F.col("current_salary_norm") >= 0)),
        ("gpa_in_range", F.col("gpa").isNull() | F.col("gpa").between(0, 4)),
    ]


def _job_rules():
    return [
        ("post_id_not_null", F.col("post_id").isNotNull()),
        ("salary_positive", F.col("salary_norm").isNull() | (F.col("salary_norm") > 0)),
        ("deadline_after_post_date",
         F.col("deadline").isNull() | F.col("post_date").isNull() | (F.col("deadline") >= F.col("post_date"))),
        ("status_valid", F.col("status").isNull() | F.col("status").isin("open", "closed")),
        ("work_mode_valid",
         F.col("work_mode").isNull() | F.lower(F.col("work_mode")).isin("remote", "hybrid", "onsite")),
    ]


def _application_rules():
    return [
        ("application_id_not_null", F.col("application_id").isNotNull()),
        ("candidate_id_not_null", F.col("candidate_id").isNotNull()),
        ("post_id_not_null", F.col("post_id").isNotNull()),
        ("applied_date_not_null", F.col("applied_date").isNotNull()),
        ("status_valid", F.col("status").isNull() | F.col("status").isin(
            "applied", "viewed", "interview", "offer", "rejected", "accepted")),
    ]


RULE_BUILDERS = {"candidate": _candidate_rules, "job": _job_rules, "application": _application_rules}
ID_COL = {"candidate": "candidate_id", "job": "post_id", "application": "application_id"}


def validate_and_split(spark, df: DataFrame, entity: str, id_col_override: str = None) -> DataFrame:
    """
    Returns only the rows that pass EVERY rule. Rows failing 1+ rules are
    written (one row per failed rule) to the quarantine table for `entity`
    and excluded from the return value. DELETE events (op='d') skip
    positive-value rules that don't apply to a tombstone-like row.
    """
    rules = RULE_BUILDERS[entity]()
    id_col = id_col_override or ID_COL[entity]

    is_delete = F.col("op") == "d"
    failures = []
    overall_valid = F.lit(True)
    for rule_name, expr in rules:
        passed = is_delete | expr  # deletes are exempt from content rules
        overall_valid = overall_valid & passed
        failure_df = (
            df.filter(~passed)
              .select(
                  F.col(id_col).alias("record_id"),
                  F.lit(entity).alias("entity"),
                  F.lit(rule_name).alias("validation_rule"),
                  F.lit(f"failed rule: {rule_name}").alias("rejection_reason"),
                  F.current_timestamp().alias("processed_at"),
              )
        )
        failures.append(failure_df)

    if failures:
        quarantine_df = failures[0]
        for f_df in failures[1:]:
            quarantine_df = quarantine_df.unionByName(f_df)
        quarantine_df = quarantine_df.dropDuplicates(["record_id", "entity", "validation_rule"])
        (
            quarantine_df.write.format("delta").mode("append")
            .option("mergeSchema", "true")
            .save(QUARANTINE_PATHS[entity])
        )

    return df.filter(overall_valid)
