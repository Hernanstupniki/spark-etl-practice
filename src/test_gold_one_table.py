from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max, row_number, desc, when, col, stddev, expr, dense_rank, abs, round, sum as sum_
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("test_gold_one_table") \
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

# Rating metrics
df_ratings_metrics = (
    df_silver_clean.agg(
        count("*").alias("total_ratings")
    )
)
# Most frequent rating
df_rating_count = (
    df_silver_clean
    .groupBy("rating")
    .agg(
        count("*").alias("rating_count"),
        )
)

window_rating_mode = Window.orderBy(desc("rating_count"))

df_gold_rating_mode = (
    df_rating_count
    .withColumn("rn", row_number().over(window_rating_mode))
    .filter("rn = 1")
    .select("rating", "rating_count")
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
    expr("collect_list(rental_rate)").alias("list_rental_rate"),
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
    .select("length", col("rating").alias("mode_rating"), col("rating_count").alias("mode_frequency"), "has_real_mode")
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

# Analitic rankings
window_ranking = Window.orderBy(desc("films"))

# Ranking more films per length
df_rankings_more_films_per_length = (
    df_silver_clean
    .groupBy("length").agg(count("*").alias("films"))
    .withColumn("rank", dense_rank().over(window_ranking)).orderBy(desc("films"))
)

# Ranking more films per ratings
df_rankings_more_films_per_ratings = (
    df_silver_clean
    .groupBy("rating").agg(count("*").alias("films"))
    .withColumn("rank", dense_rank().over(window_ranking)).orderBy(desc("films"))
)

# Stats for outliers
stats = (
    df_silver_clean.agg(
        avg("length").alias("avg_length"),
        stddev("length").alias("stddev_length")
        )
).collect()[0]

avg_length = stats["avg_length"]
stddev_length = stats["stddev_length"]

# outliers
df_outliers_length = (
    df_silver_clean
    .withColumn(
        "is_length_outlier",
        when(
            abs(col("length") - avg_length) > 2 * stddev_length,
            True
        ).otherwise(False)
    )
)

# Coverage metrics ratings
df_count_per_rating = (
    df_silver_clean
    .groupBy("rating")
    .agg(count("*").alias("count_rating"))
)

window_total = Window.partitionBy()

df_coverage_metrics_ratings_percentage = (
    df_count_per_rating
    .withColumn(
        "coverage_percentage",
        round((col("count_rating") / sum_("count_rating").over(window_total)) * 100, 2)
    ).orderBy(desc("coverage_percentage"))
)

# Coverage metrics length df_length_segmentation
df_count_per_length = (
    df_length_segmentation
    .groupBy("length_segment")
    .agg(count("*").alias("count_length_segment"))
)

df_coverage_metrics_length_percentage = (
    df_count_per_length
    .withColumn(
        "coverage_percentage",
        round((col("count_length_segment") / sum_("count_length_segment").over(window_total)) * 100, 2)
    ).orderBy(desc("coverage_percentage"))
)


df_gold_metrics.write.mode("overwrite").parquet("data/gold/film_metrics")