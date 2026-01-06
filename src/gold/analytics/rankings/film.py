"""
Gold Rankings – Film
Purpose: Ordered analytical rankings
Grain: One row per ranked entity
Source: Silver film clean
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, dense_rank, desc, lit, current_timestamp
from pyspark.sql.window import Window

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

spark = SparkSession.builder.appName("gold_rankings_film").getOrCreate()

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

def add_metadata(df, job_name, job_version="v1"):
    return (
        df
        .withColumn("job_name", lit(job_name))
        .withColumn("job_version", lit(job_version))
        .withColumn("execution_ts", current_timestamp())
    )

window_ranking = Window.orderBy(desc("films"))

# Ranking by length
df_rankings_more_films_per_length = add_metadata(
    df_silver_clean
    .groupBy("length")
    .agg(count("*").alias("films"))
    .withColumn("rank", dense_rank().over(window_ranking))
    .orderBy(desc("films")),
    "gold_rankings_more_films_per_length"
)

# Ranking by rating
df_rankings_more_films_per_rating = add_metadata(
    df_silver_clean
    .groupBy("rating")
    .agg(count("*").alias("films"))
    .withColumn("rank", dense_rank().over(window_ranking))
    .orderBy(desc("films")),
    "gold_rankings_more_films_per_rating"
)

df_rankings_more_films_per_length.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/analytics/rankings/films_per_length")

df_rankings_more_films_per_rating.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/analytics/rankings/films_per_rating")
