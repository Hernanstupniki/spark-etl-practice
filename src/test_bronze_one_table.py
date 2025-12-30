import os
from pyspark.sql import SparkSession

# CONFIG JDBC (ENV VARS)
POSTGRES_JAR = os.getenv("POSTGRES_JAR")
JDBC_URL = os.getenv("JDBC_URL")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
TABLE = "film"

# Basic validation
if not all([POSTGRES_JAR, JDBC_URL, USER, PASSWORD]):
    raise RuntimeError("Missing environment variables for JDBC connection")

# SPARK SESSION
spark = (
    SparkSession.builder
    .appName("test_bronze_one_table")
    .master("local[*]")
    .config("spark.jars", POSTGRES_JAR)
    .getOrCreate()
)

# READ JDBC (BRONZE)
df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TABLE)
    .option("user", USER)
    .option("password", PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"✅ Conectó y leyó la tabla: {TABLE}")
print("Filas:", df.count())
df.printSchema()
df.select("title").show(truncate=False)

# WRITE BRONZE (PARQUET)
output_path = f"data/bronze/{TABLE}"

df.write.mode("overwrite").parquet(output_path)

print("✅ Guardado en:", output_path)

# CLOSE
spark.stop()