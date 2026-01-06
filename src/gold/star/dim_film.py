import os
from pyspark.sql import SparkSession

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

spark = SparkSession.builder \
    .appName("test_gold_one_table") \
    .master("local[*]") \
    .getOrCreate()

spark = (
    SparkSession.builder
    .appName("gold_dim_rating")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df_silver_clean = (
    spark.read.parquet(f"{BASE_PATH}/silver/film")
    .filter("has_quality_issues = false")
)

df_dim_film = (
    df_silver_clean
    .select("film_id", "title", "release_year")
    .dropDuplicates(["film_id"])
)

df_dim_film.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/star/dim_film")

