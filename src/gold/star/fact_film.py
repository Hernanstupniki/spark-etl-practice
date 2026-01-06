import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

spark = SparkSession.builder \
    .appName("test_gold_one_table") \
    .master("local[*]") \
    .getOrCreate()

df_silver_clean = (
    spark.read.parquet(f"{BASE_PATH}/silver/film")
    .filter("has_quality_issues = false")
)

df_fact_film = (
    df_silver_clean
    .select("film_id", "rating", "length", "rental_rate")
    .withColumn("execution_ts", current_timestamp())
)

df_fact_film.write.mode("overwrite") \
    .partitionBy("rating") \
    .parquet(f"{BASE_PATH}/gold/star/fact_film")

