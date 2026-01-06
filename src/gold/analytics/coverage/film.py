"""
Gold Coverage – Film
Purpose: Distribution percentages by category
Grain: One row per category
Source: Silver film clean
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, round, sum as sum_, col, when, desc, lit, current_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("gold_coverage_film").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

window_total = Window.partitionBy()

# Coverage by rating
df_coverage_ratings = add_metadata(
    df_silver_clean
    .groupBy("rating")
    .agg(count("*").alias("count_rating"))
    .withColumn(
        "coverage_percentage",
        round(
            (col("count_rating") / sum_("count_rating").over(window_total)) * 100, 2
        )
    )
    .orderBy(desc("coverage_percentage")),
    "gold_coverage_ratings"
)

# Length segmentation
df_length_segmentation = (
    df_silver_clean
    .withColumn(
        "length_segment",
        when(col("length") < 50, "short")
        .when((col("length") >= 50) & (col("length") < 100), "medium")
        .otherwise("long")
    )
)

# Coverage by length segment
df_coverage_length_segments = add_metadata(
    df_length_segmentation
    .groupBy("length_segment")
    .agg(count("*").alias("count_length_segment"))
    .withColumn(
        "coverage_percentage",
        round(
            (col("count_length_segment") /
             sum_("count_length_segment").over(window_total)) * 100, 2
        )
    )
    .orderBy(desc("coverage_percentage")),
    "gold_coverage_length_segments"
)

df_coverage_ratings.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/coverage/ratings")

df_coverage_length_segments.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/coverage/length_segments")
