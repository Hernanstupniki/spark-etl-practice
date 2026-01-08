import os
from pyspark.sql import SparkSession

POSTGRES_JAR = os.getenv("POSTGRES_JAR")
JDBC_URL = os.getenv("JDBC_URL")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

if not all([POSTGRES_JAR, JDBC_URL, USER, PASSWORD]):
    raise RuntimeError("Missing environment variables for JDBC connection")

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

QUERY = """
(
    SELECT
        film_id,
        title,
        release_year,
        language_id,
        rental_rate,
        length,
        rating
    FROM film
) AS src
"""

spark = (
    SparkSession.builder
    .appName("bronze_film_full_sql_extract")
    .master("local[*]")
    .config("spark.jars", POSTGRES_JAR)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", QUERY)
    .option("user", USER)
    .option("password", PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print("Conected")
print("Rows:", df.count())
df.printSchema()

output_path = f"{BASE_PATH}/bronze/film"

df.write.mode("overwrite").parquet(output_path)

print("Saved in:", output_path)

spark.stop()

