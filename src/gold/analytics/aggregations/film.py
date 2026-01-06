"""
Gold Aggregations – Film
Purpose: Metrics grouped by business dimensions
Grain: One row per dimension value
Source: Silver film clean
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, expr, lit, current_timestamp

spark = SparkSession.builder.appName("gold_aggregations_film").getOrCreate()

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

df_silver_clean = (
    spark.read.parquet(f"{BASE_PATH}/silver/film")
    .filter("has_quality_issues = false")
)

def add_metadata(df, job_name, job_version="v1"):
    return (
        df
        .withColumn("job_name", lit(job_name))
        .withColumn("job_version", lit(job_version))
        .withColumn("execution_ts", current_timestamp())
    )

# Aggregation by rating
df_film_metrics_by_rating = add_metadata(
    df_silver_clean
    .groupBy("rating")
    .agg(
        count("*").alias("films_per_rating"),
        avg("rental_rate").alias("avg_rental_rate"),
        avg("length").alias("avg_length")
    ),
    "gold_film_metrics_by_rating"
)

# Aggregation by length
df_film_metrics_by_length = add_metadata(
    df_silver_clean
    .groupBy("length")
    .agg(
        count("*").alias("films_per_length"),
        avg("rental_rate").alias("avg_rental_rate"),
        expr("collect_list(rental_rate)").alias("list_rental_rate")
    ),
    "gold_film_metrics_by_length"
)

df_film_metrics_by_rating.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/aggregations/films_by_rating")

df_film_metrics_by_length.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/aggregations/films_by_length")
