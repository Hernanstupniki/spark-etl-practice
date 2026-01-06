#!/bin/bash
set -e

export SPARK_SUBMIT_OPTS="-Dlog4j.rootCategory=WARN,console"

echo "Starting Gold Analytics pipeline"
echo "ENV=${ENV:-dev}"

SPARK_CMD=${SPARK_CMD:-spark-submit}
BASE_PATH="src/gold/analytics"

echo "Running metrics"
$SPARK_CMD $BASE_PATH/metrics/film.py

echo "Running aggregations"
$SPARK_CMD $BASE_PATH/aggregations/film.py

echo "Running coverage"
$SPARK_CMD $BASE_PATH/coverage/film.py

echo "Running rankings"
$SPARK_CMD $BASE_PATH/rankings/film.py

echo "Running quality checks"
$SPARK_CMD $BASE_PATH/quality/film.py

echo "Running marts"
$SPARK_CMD $BASE_PATH/marts/film_overview.py

echo "Gold Analytics pipeline finished successfully"
