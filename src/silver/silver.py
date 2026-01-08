import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, upper, current_timestamp, lit, when

spark = SparkSession.builder \
    .appName("test_silver_one_table") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ENV = os.getenv("ENV", "dev")
BASE_PATH = f"data/{ENV}"

df_silver = spark.read.parquet(f"{BASE_PATH}/bronze/film")

df_silver = df_silver.select("film_id", "title", "release_year", "rating", "rental_rate", "length")

# Normalize data and string fields
df_silver = (
    df_silver
    .withColumn("film_id", df_silver["film_id"].cast("int"))
    .withColumn("release_year", df_silver["release_year"].cast("int"))
    .withColumn("rental_rate", df_silver["rental_rate"].cast("double"))
    .withColumn("length", df_silver["length"].cast("int"))
    .withColumn("title", trim(df_silver["title"]))
    .withColumn("rating", upper(df_silver["rating"]))
)

# Apply data quality filters for critical business rules
df_silver = df_silver.filter("film_id > 0")
df_silver = df_silver.filter("title IS NOT NULL")
df_silver = df_silver.filter("rental_rate >= 0")
df_silver = df_silver.filter("length > 0")

# Clean duplicates
df_silver = df_silver.dropDuplicates(["film_id"])


# Patch null values in non-critical fields
df_silver = df_silver.fillna({
    "rating": "UNRATED"
    })

# Silver data quality validations
# 1. Silver must not be empty
assert df_silver.count() > 0

# 2. Primary key must be valid (> 0)
assert df_silver.filter("film_id <= 0").count() == 0

# 3. Primary key must be unique
assert df_silver.count() == df_silver.dropDuplicates(["film_id"]).count()

# 4. Critical business fields must not be null
assert df_silver.filter("title IS NULL").count() == 0

# 5. Numeric business rules
assert df_silver.filter("rental_rate < 0").count() == 0
assert df_silver.filter("length <= 0").count() == 0

# Audit metadata and data quality flags
df_silver = (
    df_silver
    # Processing metadata
    .withColumn("silver_processed_at", current_timestamp())
    .withColumn("data_layer", lit("silver"))
    .withColumn("source_table", lit("film"))
    .withColumn("pipeline_version", lit("v1.0"))
    .withColumn("environment", lit("local"))

    # Data quality flags
    .withColumn(
        "has_quality_issues",
        when(df_silver["release_year"] < 1900, True).otherwise(False)
    )
    .withColumn(
        "quality_issue_reason",
        when(df_silver["release_year"] < 1900, "INVALID_RELEASE_YEAR")
        .otherwise(None)
    )
)

# Ratings validations
VALID_RATINGS = ["G", "PG", "PG-13", "R", "NC-17", "UNRATED"]

df_silver = df_silver.withColumn(
    "rating",
    when(df_silver["rating"].isin(VALID_RATINGS), df_silver["rating"])
    .otherwise("UNRATED")
)

df_silver.printSchema()

df_silver.write.mode("overwrite").parquet(f"{BASE_PATH}/silver/film")

spark.stop()

