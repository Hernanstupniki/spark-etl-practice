"""
Gold Quality – Film
Purpose: Data quality and reliability indicators
Grain: Varies by control
Source: Silver film clean
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, stddev, when, col, desc, lit, current_timestamp, row_number
from pyspark.sql.window import Window

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

spark = SparkSession.builder.appName("gold_quality_film").getOrCreate()

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

# Low data flag
MIN_SAMPLE_SIZE = 30

df_quality_low_data_by_rating = add_metadata(
    df_silver_clean
    .groupBy("rating")
    .agg(count("*").alias("films"))
    .withColumn(
        "low_data_flag",
        when(col("films") < MIN_SAMPLE_SIZE, True).otherwise(False)
    ),
    "gold_quality_low_data_by_rating"
)

# Stability flag
df_quality_stability_by_rating = add_metadata(
    df_silver_clean
    .groupBy("rating")
    .agg(
        avg("rental_rate").alias("avg_rental_rate"),
        stddev("rental_rate").alias("stddev_rental_rate"),
        count("*").alias("films")
    )
    .withColumn(
        "stable_result_flag",
        when(col("stddev_rental_rate") < 0.5, True).otherwise(False)
    ),
    "gold_quality_stability_by_rating"
)

# Mode reliability by length
df_length_rating_count = (
    df_silver_clean
    .groupBy("length", "rating")
    .agg(count("*").alias("rating_count"))
)

window_by_length = Window.partitionBy("length").orderBy(desc("rating_count"))

df_quality_mode_reliability_by_length = add_metadata(
    df_length_rating_count
    .withColumn("rn", row_number().over(window_by_length))
    .filter(col("rn") == 1)
    .withColumn(
        "mode_reliable_flag",
        when(col("rating_count") >= 3, True).otherwise(False)
    )
    .select(
        "length",
        col("rating").alias("mode_rating"),
        col("rating_count").alias("mode_frequency"),
        "mode_reliable_flag"
    ),
    "gold_quality_mode_reliability_by_length"
)

df_quality_low_data_by_rating.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/quality/low_data_by_rating")

df_quality_stability_by_rating.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/quality/stability_by_rating")

df_quality_mode_reliability_by_length.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/quality/mode_reliability_by_length")
