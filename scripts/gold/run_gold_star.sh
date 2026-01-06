#!/bin/bash
set -e

export SPARK_SUBMIT_OPTS="-Dlog4j.rootCategory=WARN,console"

echo "Starting Gold STAR pipeline"
echo "ENV=${ENV:-dev}"

# Command to run Spark jobs (can be overridden)
SPARK_CMD=${SPARK_CMD:-spark-submit}

STAR_PATH="src/gold/star"

echo "Running dimension: dim_rating"
$SPARK_CMD $STAR_PATH/dim_rating.py

echo "Running dimension: dim_length_segment"
$SPARK_CMD $STAR_PATH/dim_length_segment.py

echo "Running dimension: dim_film"
$SPARK_CMD $STAR_PATH/dim_film.py

echo "Running fact table: fact_film"
$SPARK_CMD $STAR_PATH/fact_film.py

echo "Gold STAR pipeline finished successfully"
