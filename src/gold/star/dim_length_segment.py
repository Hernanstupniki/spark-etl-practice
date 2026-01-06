import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

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

df_dim_length_segment = (
    df_silver_clean
    .withColumn(
        "length_segment",
        when(col("length") < 50, "short")
        .when((col("length") < 100), "medium")
        .otherwise("long")
    )
    .select("length", "length_segment")
    .dropDuplicates()
)

df_dim_length_segment.write.mode("overwrite") \
    .parquet(f"{BASE_PATH}/gold/star/dim_length_segment")
