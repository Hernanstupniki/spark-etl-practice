import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test_gold_one_table") \
    .master("local[*]") \
    .getOrCreate()

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

df_silver_clean = (
    spark.read.parquet(f"{BASE_PATH}/silver/film")
    .filter("has_quality_issues = false")
)

df_dim_rating = (
    df_silver_clean
    .select("rating")
    .dropDuplicates()
)

df_dim_rating.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/star/dim_rating")
