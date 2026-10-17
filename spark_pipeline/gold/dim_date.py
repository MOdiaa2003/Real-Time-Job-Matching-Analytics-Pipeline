"""Gold — dim_date. Standard calendar dimension for BI tools to join on."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config import GOLD_DIM_DATE


def build_dim_date(spark: SparkSession, start="2023-01-01", end="2030-12-31"):
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_key")
    df = (
        df.withColumn("year", F.year("date_key"))
          .withColumn("quarter", F.quarter("date_key"))
          .withColumn("month", F.month("date_key"))
          .withColumn("month_name", F.date_format("date_key", "MMMM"))
          .withColumn("day_of_month", F.dayofmonth("date_key"))
          .withColumn("day_of_week", F.dayofweek("date_key"))
          .withColumn("day_name", F.date_format("date_key", "EEEE"))
          .withColumn("week_of_year", F.weekofyear("date_key"))
          .withColumn("is_weekend", F.dayofweek("date_key").isin(1, 7))
    )
    df.write.format("delta").mode("overwrite").save(GOLD_DIM_DATE)
