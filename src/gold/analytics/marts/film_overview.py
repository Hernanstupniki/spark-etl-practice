"""
Gold Mart – Film Overview
Purpose: BI-ready dataset for film analysis by rating
Grain: One row per rating
Source: Gold aggregations, quality, coverage, rankings
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment configuration
ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

logger.info(f"Starting Gold Mart: film_overview | ENV={ENV}")

# Spark session
spark = (
    SparkSession.builder
    .appName("gold_mart_film_overview")
    .getOrCreate()
)

# Metadata columns inherited from gold datasets
METADATA_COLS = ["job_name", "job_version", "execution_ts"]

# Read gold aggregations
logger.info("Reading gold datasets")

df_film_metrics_by_rating = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/aggregations/films_by_rating")
    .drop(*METADATA_COLS)
)

# Read quality datasets
df_quality_low_data = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/quality/low_data_by_rating")
    .drop(*METADATA_COLS)
)

df_quality_stability = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/quality/stability_by_rating")
    .select("rating", "stable_result_flag")
)

# Read coverage dataset
df_coverage_ratings = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/coverage/ratings")
    .select("rating", "coverage_percentage")
)

# Read rankings dataset
df_rankings = (
    spark.read.parquet(f"{BASE_PATH}/gold/analytics/rankings/films_per_rating")
    .select("rating", "rank")
)

# Derive dominance flag based on coverage threshold
logger.info("Deriving dominance flag")

df_dominance = (
    df_coverage_ratings
    .withColumn(
        "dominant_category_flag",
        col("coverage_percentage") >= 40
    )
)

# Build final mart by joining all gold datasets
logger.info("Building film_overview mart")

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

# Log number of rows produced
rows = df_gold_film_overview.count()
logger.info(f"Rows generated for mart: {rows}")

# Add mart-level metadata (single metadata layer)
df_gold_film_overview = (
    df_gold_film_overview
    .withColumn("job_name", lit("gold_mart_film_overview"))
    .withColumn("job_version", lit("v1"))
    .withColumn("execution_ts", current_timestamp())
)

# Data quality check: business key must not be null
nulls = df_gold_film_overview.filter(col("rating").isNull()).count()

if nulls > 0:
    logger.error("Data quality check failed: NULL ratings found")
    raise Exception("Null ratings found in mart")

logger.info("Data quality checks passed")

# Write mart to gold layer
output_path = f"{BASE_PATH}/gold/analytics/marts/film_overview"

logger.info(f"Writing mart to {output_path}")

df_gold_film_overview.write.mode("overwrite").parquet(output_path)

logger.info("Gold mart film_overview written successfully")

# Stop Spark session
spark.stop()
logger.info("Spark session stopped")
