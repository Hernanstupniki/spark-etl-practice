"""
Gold Insights – Film
Purpose: Derived analytical insights
Grain: Varies (global / per segment)
Source: Silver film clean
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, row_number, desc, expr, lit, current_timestamp
from pyspark.sql.window import Window

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

spark = SparkSession.builder.appName("gold_insights_film").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

# Global rating mode
df_rating_count = df_silver_clean.groupBy("rating").agg(count("*").alias("rating_count"))
window_global = Window.orderBy(desc("rating_count"))

df_most_rating_mode = add_metadata(
    df_rating_count
    .withColumn("rn", row_number().over(window_global))
    .filter("rn = 1")
    .select("rating", "rating_count"),
    "gold_most_rating_mode"
)

# Rating mode by length
df_length_rating_count = (
    df_silver_clean
    .groupBy("length", "rating")
    .agg(count("*").alias("rating_count"))
)

window_by_length = Window.partitionBy("length").orderBy(desc("rating_count"))

df_rating_mode_by_length = add_metadata(
    df_length_rating_count
    .withColumn("rn", row_number().over(window_by_length))
    .filter("rn = 1")
    .select("length", "rating", "rating_count"),
    "gold_rating_mode_by_length"
)

# Percentiles
df_length_percentiles = add_metadata(
    df_silver_clean.agg(
        expr("percentile_approx(length, 0.25)").alias("p25_length"),
        expr("percentile_approx(length, 0.50)").alias("median_length"),
        expr("percentile_approx(length, 0.75)").alias("p75_length")
    ),
    "gold_length_percentiles"
)

df_rental_rate_percentiles = add_metadata(
    df_silver_clean.agg(
        expr("percentile_approx(rental_rate, 0.25)").alias("p25_rental_rate"),
        expr("percentile_approx(rental_rate, 0.50)").alias("median_rental_rate"),
        expr("percentile_approx(rental_rate, 0.75)").alias("p75_rental_rate")
    ),
    "gold_rental_rate_percentiles"
)

df_most_rating_mode.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/analytics/insights/global_rating_mode")
df_rating_mode_by_length.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/analytics/insights/rating_mode_by_length")
df_length_percentiles.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/analytics/insights/length_percentiles")
df_rental_rate_percentiles.write.mode("overwrite").parquet(f"{BASE_PATH}/gold/analytics/insights/rental_rate_percentiles")
