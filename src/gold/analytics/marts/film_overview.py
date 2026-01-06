"""
Gold Mart – Film Overview
Purpose: BI-ready dataset for film analysis by rating
Grain: One row per rating
Source: Gold aggregations, quality, coverage, rankings
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

# --------------------------------------------------
# Environment
# --------------------------------------------------
ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

# --------------------------------------------------
# Spark
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("gold_mart_film_overview")
    .getOrCreate()
)

# --------------------------------------------------
# Constants
# --------------------------------------------------
METADATA_COLS = ["job_name", "job_version", "execution_ts"]

# --------------------------------------------------
# Read Gold datasets
# --------------------------------------------------

# Base aggregations
df_film_metrics_by_rating = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/aggregations/films_by_rating")
    .drop(*METADATA_COLS)
)

# Quality flags
df_quality_low_data = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/quality/low_data_by_rating")
    .drop(*METADATA_COLS)
)

df_quality_stability = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/quality/stability_by_rating")
    .select("rating", "stable_result_flag")
)

# Coverage
df_coverage_ratings = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/coverage/ratings")
    .select("rating", "coverage_percentage")
)

# Rankings
df_rankings = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/rankings/films_per_rating")
    .select("rating", "rank")
)

# --------------------------------------------------
# Derived flags
# --------------------------------------------------

df_dominance = (
    df_coverage_ratings
    .withColumn(
        "dominant_category_flag",
        df_coverage_ratings.coverage_percentage >= 40
    )
)

# --------------------------------------------------
# Build mart
# --------------------------------------------------

df_gold_film_overview = (
    df_film_metrics_by_rating
    .join(df_quality_low_data, "rating", "left")
    .join(df_quality_stability, "rating", "left")
    .join(df_coverage_ratings, "rating", "left")
    .join(df_rankings, "rating", "left")
    .join(
        df_dominance.select("rating", "dominant_category_flag"),
        "rating",
        "left"
    )
)

# --------------------------------------------------
# Add mart metadata (single metadata layer)
# --------------------------------------------------

df_gold_film_overview = (
    df_gold_film_overview
    .withColumn("job_name", lit("gold_mart_film_overview"))
    .withColumn("job_version", lit("v1"))
    .withColumn("execution_ts", current_timestamp())
)

# --------------------------------------------------
# Write mart
# --------------------------------------------------

df_gold_film_overview.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/marts/film_overview")

spark.stop()
