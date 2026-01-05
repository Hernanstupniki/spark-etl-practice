"""
Gold Metrics – Film
Purpose: Global KPIs for BI cards
Grain: Single row
Source: Silver film clean
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max, lit, current_timestamp

spark = SparkSession.builder.appName("gold_metrics_film").getOrCreate()

df_silver_clean = (
    spark.read.parquet("data/silver/film")
    .filter("has_quality_issues = false")
)

def add_metadata(df, job_name, job_version="v1"):
    return (
        df
        .withColumn("job_name", lit(job_name))
        .withColumn("job_version", lit(job_version))
        .withColumn("execution_ts", current_timestamp())
    )

df_length_metrics = add_metadata(
    df_silver_clean.agg(
        count("*").alias("total_films"),
        avg("length").alias("avg_length"),
        min("length").alias("min_length"),
        max("length").alias("max_length")
    ),
    "gold_film_length_metrics"
)

df_rental_rate_metrics = add_metadata(
    df_silver_clean.agg(
        count("*").alias("total_films"),
        avg("rental_rate").alias("avg_rental_rate"),
        min("rental_rate").alias("min_rental_rate"),
        max("rental_rate").alias("max_rental_rate")
    ),
    "gold_rental_rate_metrics"
)

df_length_metrics.write.mode("overwrite") \
    .parquet("data/gold/metrics/film_length")

df_rental_rate_metrics.write.mode("overwrite") \
    .parquet("data/gold/metrics/rental_rate")
