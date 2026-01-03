from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("inspect_bronze_film") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/bronze/film")

print("=== SCHEMA ===")
df.printSchema()

print("=== SAMPLE ===")
df.select(
    "film_id",
    "title",
    "release_year",
    "rating",
    "rental_rate"
).show(10, truncate=False)

print("=== STADISTICS ===")
df.describe("rental_rate", "length").show()

spark.stop()