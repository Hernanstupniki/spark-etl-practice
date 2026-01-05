"""
Gold Mart – Film Overview
Purpose: BI-ready dataset for film analysis by rating
Grain: One row per rating
Source: Gold aggregations, quality, coverage, rankings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

spark = SparkSession.builder \
    .appName("gold_mart_film_overview") \
    .getOrCreate()

# --------------------------------------------------
# Constants
# --------------------------------------------------
METADATA_COLS = ["job_name", "job_version", "execution_ts"]

# --------------------------------------------------
# Read Gold datasets
# --------------------------------------------------

# Base aggregations
df_film_metrics_by_rating = (
    spark.read.parquet("data/gold/aggregations/films_by_rating")
    .drop(*METADATA_COLS)
)

# Quality flags
df_quality_low_data = (
    spark.read.parquet("data/gold/quality/low_data_by_rating")
    .drop(*METADATA_COLS)
)

df_quality_stability = (
    spark.read.parquet("data/gold/quality/stability_by_rating")
    .select("rating", "stable_result_flag")
)

# Coverage
df_coverage_ratings = (
    spark.read.parquet("data/gold/coverage/ratings")
    .select("rating", "coverage_percentage")
)

# Rankings
df_rankings = (
    spark.read.parquet("data/gold/rankings/films_per_rating")
    .select("rating", "rank")
)

# --------------------------------------------------
# Derived flags
# --------------------------------------------------

# Dominance flag (from coverage)
df_dominance = (
    df_coverage_ratings
    .withColumn(
        "dominant_category_flag",
        df_coverage_ratings.coverage_percentage >= 40
    )
)


# Build mart
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


# Add mart metadata (ONE single metadata)
df_gold_film_overview = (
    df_gold_film_overview
    .withColumn("job_name", lit("gold_mart_film_overview"))
    .withColumn("job_version", lit("v1"))
    .withColumn("execution_ts", current_timestamp())
)

# Write mart
df_gold_film_overview.write.mode("overwrite") \
    .parquet("data/gold/marts/film_overview")

"""
Gold Mart – Film Overview
Purpose: BI-ready dataset for film analysis by rating
Grain: One row per rating
Source: Gold aggregations, quality, coverage, rankings
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

spark = SparkSession.builder \
    .appName("gold_mart_film_overview") \
    .getOrCreate()

# --------------------------------------------------
# Constants
# --------------------------------------------------
METADATA_COLS = ["job_name", "job_version", "execution_ts"]

# --------------------------------------------------
# Read Gold datasets
# --------------------------------------------------

# Base aggregations
df_film_metrics_by_rating = (
    spark.read.parquet("data/gold/aggregations/films_by_rating")
    .drop(*METADATA_COLS)
)

# Quality flags
df_quality_low_data = (
    spark.read.parquet("data/gold/quality/low_data_by_rating")
    .drop(*METADATA_COLS)
)

df_quality_stability = (
    spark.read.parquet("data/gold/quality/stability_by_rating")
    .select("rating", "stable_result_flag")
)

# Coverage
df_coverage_ratings = (
    spark.read.parquet("data/gold/coverage/ratings")
    .select("rating", "coverage_percentage")
)

# Rankings
df_rankings = (
    spark.read.parquet("data/gold/rankings/films_per_rating")
    .select("rating", "rank")
)

# --------------------------------------------------
# Derived flags
# --------------------------------------------------

# Dominance flag (from coverage)
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
# Add mart metadata (ONE single metadata)
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
    .parquet("data/gold/marts/film_overview")