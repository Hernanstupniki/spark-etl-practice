from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max, row_number, desc, when, col, stddev, expr
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("test_silver_one_table") \
    .master("local[*]") \
    .getOrCreate()

df_silver_clean = (
    spark.read.parquet("data/silver/film")
    .filter("has_quality_issues = false")
    )

# Global film catalog metrics
df_gold_metrics = df_silver_clean.agg(
    count("*").alias("total_films"),
    avg("length").alias("avg_length"),
    avg("rental_rate").alias("avg_rental_rate"),
    min("length").alias("min_length"),
    max("length").alias("max_length")
    )

# Film metrics by rating
df_gold_by_rating = df_silver_clean.groupBy("rating").agg(
    count("*").alias("films_per_ratings"),
    avg("rental_rate").alias("avg_rental_rate"),
    avg("length").alias("avg_length")
)

# Film metrics by length
df_gold_by_lenght = df_silver_clean.groupBy("length").agg(
    count("*").alias("films_per_lenght"),
    avg("rental_rate").alias("avg_rental_rate"),
)

# Most frequent rating per length
df_length_rating_count = (
    df_silver_clean
    .groupBy("length", "rating")
    .agg(count("*").alias("rating_count"))
)

window = Window.partitionBy("length").orderBy(desc("rating_count"))

df_gold_rating_mode_by_length = (
    df_length_rating_count
    .withColumn("rn", row_number().over(window))
    .filter("rn = 1")
    .select("length", "rating", "rating_count")
)

# Rating mode reliability per length
df_mode_reliability = (
    df_length_rating_count
    .withColumn("rn", row_number().over(window))
    .filter("rn = 1")
    .withColumn(
        ("has_real_mode"),
        when(col("rating_count") > 1, True).otherwise(False)
    )
    .select("length", "rating", "rating_count", "has_real_mode")
    )


# standar deviation
df_standard_deviation = df_silver_clean.agg(
    stddev("length").alias("stddev_length"),
    stddev("rental_rate").alias("stddev_rental_rate"),
)

# Length percentiles
df_gold_length_percentiles = df_silver_clean.agg(
    expr("percentile_approx(length, 0.25)").alias("p25_length"),
    expr("percentile_approx(length, 0.50)").alias("median_length"),
    expr("percentile_approx(length, 0.75)").alias("p75_length")
)

# rental rate percentiles
df_gold_rental_rate_percentiles = df_silver_clean.agg(
    expr("percentile_approx(rental_rate, 0.25)").alias("p25_rental_rate"),
    expr("percentile_approx(rental_rate, 0.50)").alias("median_rental_rate"),
    expr("percentile_approx(rental_rate, 0.75)").alias("p75_rental_rate")
)

# Length segmentation
df_length_segmentation = (
    df_silver_clean
    .withColumn(
    "length_segment",
    when(col("length") < 50, "short")
    .when((col("length") >= 50) & (col("length") < 100), "medium")
    .otherwise("long"))
    )


df_length_segmentation.select("film_id", "title", "length_segment").show(200, truncate = False)

df_gold_metrics.write.mode("overwrite").parquet("data/gold/film_metrics")